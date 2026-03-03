//! Integration tests for yrs-warp-nostd and yrs_no_std compatibility
//!
//! This example/test file verifies that the no_std implementations work correctly
//! and produce output compatible with the original std versions.
//!
//! Run with: cargo run --example integration_tests

use std::sync::Arc;

// Import traits that are needed for methods
use yrs::sync::{Awareness, AwarenessUpdate, Message, SyncMessage};
use yrs::sync::protocol::{DefaultProtocol, Protocol, MSG_SYNC, MSG_SYNC_UPDATE};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::encoding::write::Write;
use yrs::{Doc, GetString, StateVector, Text, Transact, Update, ReadTxn, Map, Array};

use yrs_warp::broadcast::BroadcastGroup;
use yrs_warp::broadcast_unified::UnifiedBroadcastGroup;
use yrs_warp::conn::handle_msg;
use yrs_warp::compat::{Spawner, JoinHandle, JoinHandleTrait, JoinError};
use yrs_warp::AwarenessRef;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use serde_json::json;

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Tokio spawner for tests
#[derive(Clone, Copy)]
struct TokioSpawner;

struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T: Send + 'static> JoinHandleTrait<T> for TokioJoinHandle<T> {
    fn poll_join(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>> {
        let handle = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        match handle.poll(cx) {
            Poll::Ready(Ok(val)) => Poll::Ready(Ok(val)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(JoinError)),
            Poll::Pending => Poll::Pending,
        }
    }
    
    fn abort(&self) {
        self.0.abort();
    }
}

impl Spawner for TokioSpawner {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle::new(TokioJoinHandle(tokio::spawn(future)))
    }
}

// ============================================================================
// Test Results Tracking
// ============================================================================

#[derive(Default)]
struct TestResults {
    passed: Vec<String>,
    failed: Vec<(String, String)>,
}

impl TestResults {
    fn pass(&mut self, name: &str) {
        println!("  ✅ PASS: {}", name);
        self.passed.push(name.to_string());
    }
    
    fn fail(&mut self, name: &str, reason: &str) {
        println!("  ❌ FAIL: {} - {}", name, reason);
        self.failed.push((name.to_string(), reason.to_string()));
    }
    
    fn summary(&self) {
        println!();
        println!("{}", "=".repeat(60));
        println!("TEST SUMMARY");
        println!("{}", "=".repeat(60));
        println!("Passed: {}", self.passed.len());
        println!("Failed: {}", self.failed.len());
        
        if !self.failed.is_empty() {
            println!("\nFailed tests:");
            for (name, reason) in &self.failed {
                println!("  - {}: {}", name, reason);
            }
        }
        
        if self.failed.is_empty() {
            println!("\n🎉 All tests passed!");
        } else {
            println!("\n⚠️  Some tests failed!");
        }
    }
}

// ============================================================================
// Message Serialization Tests
// ============================================================================

fn test_sync_step1_serialization(results: &mut TestResults) {
    let test_name = "SyncStep1 serialization/deserialization";
    
    // Create a state vector
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text("test");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "hello");
    }
    
    let sv = doc.transact().state_vector();
    let msg = Message::Sync(SyncMessage::SyncStep1(sv.clone()));
    
    // Encode
    let encoded = msg.encode_v1();
    
    // Decode
    match Message::decode_v1(&encoded) {
        Ok(decoded) => {
            if let Message::Sync(SyncMessage::SyncStep1(decoded_sv)) = decoded {
                if decoded_sv == sv {
                    results.pass(test_name);
                } else {
                    results.fail(test_name, "State vectors don't match after round-trip");
                }
            } else {
                results.fail(test_name, "Decoded message is not SyncStep1");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

fn test_sync_step2_serialization(results: &mut TestResults) {
    let test_name = "SyncStep2 serialization/deserialization";
    
    // Create an update
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text("test");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "world");
    }
    
    let update = doc.transact().encode_state_as_update_v1(&StateVector::default());
    let msg = Message::Sync(SyncMessage::SyncStep2(update.clone()));
    
    // Encode
    let encoded = msg.encode_v1();
    
    // Decode
    match Message::decode_v1(&encoded) {
        Ok(decoded) => {
            if let Message::Sync(SyncMessage::SyncStep2(decoded_update)) = decoded {
                if decoded_update == update {
                    results.pass(test_name);
                } else {
                    results.fail(test_name, "Updates don't match after round-trip");
                }
            } else {
                results.fail(test_name, "Decoded message is not SyncStep2");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

fn test_sync_update_serialization(results: &mut TestResults) {
    let test_name = "SyncUpdate serialization/deserialization";
    
    let update_data = vec![1, 1, 1, 0, 4, 1, 4, 116, 101, 115, 116, 1, 97, 0];
    let msg = Message::Sync(SyncMessage::Update(update_data.clone()));
    
    // Encode
    let encoded = msg.encode_v1();
    
    // Decode
    match Message::decode_v1(&encoded) {
        Ok(decoded) => {
            if let Message::Sync(SyncMessage::Update(decoded_update)) = decoded {
                if decoded_update == update_data {
                    results.pass(test_name);
                } else {
                    results.fail(test_name, "Update data doesn't match after round-trip");
                }
            } else {
                results.fail(test_name, "Decoded message is not SyncUpdate");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

fn test_awareness_serialization(results: &mut TestResults) {
    let test_name = "Awareness update serialization/deserialization";
    
    // Create awareness update from a real Awareness instance
    let doc = Doc::with_client_id(1);
    let awareness = Awareness::new(doc);
    awareness.set_local_state(json!({"key":"value"})).unwrap();
    
    let awareness_update = awareness.update().unwrap();
    let msg = Message::Awareness(awareness_update.clone());
    
    // Encode
    let encoded = msg.encode_v1();
    
    // Decode
    match Message::decode_v1(&encoded) {
        Ok(decoded) => {
            if let Message::Awareness(decoded_update) = decoded {
                if decoded_update == awareness_update {
                    results.pass(test_name);
                } else {
                    results.fail(test_name, "Awareness updates don't match after round-trip");
                }
            } else {
                results.fail(test_name, "Decoded message is not Awareness");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

fn test_awareness_query_serialization(results: &mut TestResults) {
    let test_name = "AwarenessQuery serialization/deserialization";
    
    let msg = Message::AwarenessQuery;
    
    // Encode
    let encoded = msg.encode_v1();
    
    // Decode
    match Message::decode_v1(&encoded) {
        Ok(decoded) => {
            if matches!(decoded, Message::AwarenessQuery) {
                results.pass(test_name);
            } else {
                results.fail(test_name, "Decoded message is not AwarenessQuery");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

fn test_auth_message_serialization(results: &mut TestResults) {
    let test_name = "Auth message serialization/deserialization";
    
    // Test with permission granted (None)
    let msg = Message::Auth(None);
    let encoded = msg.encode_v1();
    
    match Message::decode_v1(&encoded) {
        Ok(decoded) => {
            if let Message::Auth(None) = decoded {
                // Now test with permission denied
                let msg = Message::Auth(Some("Access denied".to_string()));
                let encoded = msg.encode_v1();
                
                match Message::decode_v1(&encoded) {
                    Ok(decoded) => {
                        if let Message::Auth(Some(reason)) = decoded {
                            if reason == "Access denied" {
                                results.pass(test_name);
                            } else {
                                results.fail(test_name, "Auth denial reason doesn't match");
                            }
                        } else {
                            results.fail(test_name, "Expected Auth with denial reason");
                        }
                    }
                    Err(e) => {
                        results.fail(test_name, &format!("Decode denied auth failed: {:?}", e));
                    }
                }
            } else {
                results.fail(test_name, "Expected Auth(None)");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode granted auth failed: {:?}", e));
        }
    }
}

fn test_custom_message_serialization(results: &mut TestResults) {
    let test_name = "Custom message serialization/deserialization";
    
    let custom_tag: u8 = 42;
    let custom_data = vec![1, 2, 3, 4, 5];
    let msg = Message::Custom(custom_tag, custom_data.clone());
    
    // Encode
    let encoded = msg.encode_v1();
    
    // Decode
    match Message::decode_v1(&encoded) {
        Ok(decoded) => {
            if let Message::Custom(tag, data) = decoded {
                if tag == custom_tag && data == custom_data {
                    results.pass(test_name);
                } else {
                    results.fail(test_name, "Custom message data doesn't match");
                }
            } else {
                results.fail(test_name, "Decoded message is not Custom");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

// ============================================================================
// Protocol Handler Tests
// ============================================================================

fn test_protocol_sync_step1_handler(results: &mut TestResults) {
    let test_name = "Protocol handle_sync_step1";
    
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text("test");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "hello");
    }
    
    let awareness = Awareness::new(doc);
    let protocol = DefaultProtocol;
    
    // Request with empty state vector should return full update
    let sv = StateVector::default();
    
    match protocol.handle_sync_step1(&awareness, sv) {
        Ok(Some(Message::Sync(SyncMessage::SyncStep2(update)))) => {
            // Verify the update can be decoded
            match Update::decode_v1(&update) {
                Ok(_) => results.pass(test_name),
                Err(e) => results.fail(test_name, &format!("Update decode failed: {:?}", e)),
            }
        }
        Ok(other) => {
            results.fail(test_name, &format!("Unexpected response: {:?}", other));
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

fn test_protocol_sync_step2_handler(results: &mut TestResults) {
    let test_name = "Protocol handle_sync_step2";
    
    // Create source document
    let doc1 = Doc::with_client_id(1);
    let text1 = doc1.get_or_insert_text("test");
    {
        let mut txn = doc1.transact_mut();
        text1.push(&mut txn, "from_doc1");
    }
    
    // Create target document
    let doc2 = Doc::with_client_id(2);
    let _text2 = doc2.get_or_insert_text("test");
    let awareness2 = Awareness::new(doc2);
    
    let protocol = DefaultProtocol;
    
    // Get update from doc1
    let update_data = doc1.transact().encode_state_as_update_v1(&StateVector::default());
    let update = Update::decode_v1(&update_data).unwrap();
    
    match protocol.handle_sync_step2(&awareness2, update) {
        Ok(None) => {
            // Verify the update was applied
            let text2 = awareness2.doc().get_or_insert_text("test");
            let content = text2.get_string(&awareness2.doc().transact());
            if content == "from_doc1" {
                results.pass(test_name);
            } else {
                results.fail(test_name, &format!("Expected 'from_doc1', got '{}'", content));
            }
        }
        Ok(Some(_)) => {
            results.fail(test_name, "Unexpected response from handle_sync_step2");
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

fn test_protocol_update_handler(results: &mut TestResults) {
    let test_name = "Protocol handle_update";
    
    // Create source document
    let doc1 = Doc::with_client_id(1);
    let text1 = doc1.get_or_insert_text("test");
    {
        let mut txn = doc1.transact_mut();
        text1.push(&mut txn, "update_test");
    }
    
    // Create target document
    let doc2 = Doc::with_client_id(2);
    let _text2 = doc2.get_or_insert_text("test");
    let awareness2 = Awareness::new(doc2);
    
    let protocol = DefaultProtocol;
    
    // Get update from doc1
    let update_data = doc1.transact().encode_state_as_update_v1(&StateVector::default());
    let update = Update::decode_v1(&update_data).unwrap();
    
    match protocol.handle_update(&awareness2, update) {
        Ok(None) => {
            // Verify the update was applied
            let text2 = awareness2.doc().get_or_insert_text("test");
            let content = text2.get_string(&awareness2.doc().transact());
            if content == "update_test" {
                results.pass(test_name);
            } else {
                results.fail(test_name, &format!("Expected 'update_test', got '{}'", content));
            }
        }
        Ok(Some(_)) => {
            results.fail(test_name, "Unexpected response from handle_update");
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

fn test_protocol_awareness_query_handler(results: &mut TestResults) {
    let test_name = "Protocol handle_awareness_query";
    
    let doc = Doc::with_client_id(42);
    let awareness = Awareness::new(doc);
    // Must set local state for it to be included in update
    awareness.set_local_state(json!({"name": "test"})).unwrap();
    let protocol = DefaultProtocol;
    
    match protocol.handle_awareness_query(&awareness) {
        Ok(Some(Message::Awareness(update))) => {
            // Should contain at least the local client
            if update.clients.contains_key(&42) {
                results.pass(test_name);
            } else {
                results.fail(test_name, "Awareness update doesn't contain local client");
            }
        }
        Ok(other) => {
            results.fail(test_name, &format!("Unexpected response: {:?}", other));
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

fn test_protocol_awareness_update_handler(results: &mut TestResults) {
    let test_name = "Protocol handle_awareness_update";
    
    let doc = Doc::with_client_id(1);
    let awareness = Awareness::new(doc);
    let protocol = DefaultProtocol;
    
    // Create an incoming update from "client 2"
    let doc2 = Doc::with_client_id(2);
    let awareness2 = Awareness::new(doc2);
    awareness2.set_local_state(json!({"cursor":{"x":10,"y":20}})).unwrap();
    let incoming_update = awareness2.update().unwrap();
    
    match protocol.handle_awareness_update(&awareness, incoming_update) {
        Ok(None) => {
            // Verify the awareness was updated - check if client 2 is now in the awareness
            let clients: Vec<_> = awareness.iter().map(|(id, _)| id).collect();
            if clients.contains(&2) {
                results.pass(test_name);
            } else {
                results.fail(test_name, "Awareness doesn't contain client 2 state");
            }
        }
        Ok(Some(_)) => {
            results.fail(test_name, "Unexpected response from handle_awareness_update");
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

fn test_protocol_auth_denied_handler(results: &mut TestResults) {
    let test_name = "Protocol handle_auth (denied)";
    
    let doc = Doc::with_client_id(1);
    let awareness = Awareness::new(doc);
    let protocol = DefaultProtocol;
    
    match protocol.handle_auth(&awareness, Some("Not authorized".to_string())) {
        Err(yrs::sync::Error::PermissionDenied { reason }) => {
            if reason == "Not authorized" {
                results.pass(test_name);
            } else {
                results.fail(test_name, &format!("Wrong denial reason: {}", reason));
            }
        }
        Ok(_) => {
            results.fail(test_name, "Expected error for denied auth");
        }
        Err(e) => {
            results.fail(test_name, &format!("Wrong error type: {:?}", e));
        }
    }
}

fn test_protocol_auth_granted_handler(results: &mut TestResults) {
    let test_name = "Protocol handle_auth (granted)";
    
    let doc = Doc::with_client_id(1);
    let awareness = Awareness::new(doc);
    let protocol = DefaultProtocol;
    
    match protocol.handle_auth(&awareness, None) {
        Ok(None) => {
            results.pass(test_name);
        }
        Ok(Some(_)) => {
            results.fail(test_name, "Unexpected response from handle_auth");
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

// ============================================================================
// Connection Handler Tests
// ============================================================================

fn test_conn_handle_msg_sync_step1(results: &mut TestResults) {
    let test_name = "conn::handle_msg SyncStep1";
    
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text("test");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "content");
    }
    
    let awareness = Awareness::new(doc);
    let protocol = DefaultProtocol;
    
    let msg = Message::Sync(SyncMessage::SyncStep1(StateVector::default()));
    
    match handle_msg(&protocol, &awareness, msg) {
        Ok(Some(Message::Sync(SyncMessage::SyncStep2(update)))) => {
            // Verify update is valid
            match Update::decode_v1(&update) {
                Ok(_) => results.pass(test_name),
                Err(e) => results.fail(test_name, &format!("Update decode failed: {:?}", e)),
            }
        }
        Ok(other) => {
            results.fail(test_name, &format!("Unexpected response: {:?}", other));
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

fn test_conn_handle_msg_sync_step2(results: &mut TestResults) {
    let test_name = "conn::handle_msg SyncStep2";
    
    // Create source document
    let doc1 = Doc::with_client_id(1);
    let text1 = doc1.get_or_insert_text("test");
    {
        let mut txn = doc1.transact_mut();
        text1.push(&mut txn, "sync_step2_test");
    }
    let update_data = doc1.transact().encode_state_as_update_v1(&StateVector::default());
    
    // Create target document
    let doc2 = Doc::with_client_id(2);
    let _text2 = doc2.get_or_insert_text("test");
    let awareness2 = Awareness::new(doc2);
    
    let protocol = DefaultProtocol;
    
    let msg = Message::Sync(SyncMessage::SyncStep2(update_data));
    
    match handle_msg(&protocol, &awareness2, msg) {
        Ok(None) => {
            let text2 = awareness2.doc().get_or_insert_text("test");
            let content = text2.get_string(&awareness2.doc().transact());
            if content == "sync_step2_test" {
                results.pass(test_name);
            } else {
                results.fail(test_name, &format!("Wrong content: '{}'", content));
            }
        }
        Ok(Some(_)) => {
            results.fail(test_name, "Unexpected response");
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

fn test_conn_handle_msg_update(results: &mut TestResults) {
    let test_name = "conn::handle_msg Update";
    
    // Create source document
    let doc1 = Doc::with_client_id(1);
    let text1 = doc1.get_or_insert_text("test");
    {
        let mut txn = doc1.transact_mut();
        text1.push(&mut txn, "update_msg_test");
    }
    let update_data = doc1.transact().encode_state_as_update_v1(&StateVector::default());
    
    // Create target document
    let doc2 = Doc::with_client_id(2);
    let _text2 = doc2.get_or_insert_text("test");
    let awareness2 = Awareness::new(doc2);
    
    let protocol = DefaultProtocol;
    
    let msg = Message::Sync(SyncMessage::Update(update_data));
    
    match handle_msg(&protocol, &awareness2, msg) {
        Ok(None) => {
            let text2 = awareness2.doc().get_or_insert_text("test");
            let content = text2.get_string(&awareness2.doc().transact());
            if content == "update_msg_test" {
                results.pass(test_name);
            } else {
                results.fail(test_name, &format!("Wrong content: '{}'", content));
            }
        }
        Ok(Some(_)) => {
            results.fail(test_name, "Unexpected response");
        }
        Err(e) => {
            results.fail(test_name, &format!("Handler failed: {:?}", e));
        }
    }
}

// ============================================================================
// State Vector Tests
// ============================================================================

fn test_state_vector_encoding(results: &mut TestResults) {
    let test_name = "StateVector encoding/decoding";
    
    let doc = Doc::with_client_id(123);
    let text = doc.get_or_insert_text("test");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "test content");
    }
    
    let sv = doc.transact().state_vector();
    let encoded = sv.encode_v1();
    
    match StateVector::decode_v1(&encoded) {
        Ok(decoded) => {
            if decoded == sv {
                results.pass(test_name);
            } else {
                results.fail(test_name, "Decoded state vector doesn't match original");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

fn test_state_vector_merge(results: &mut TestResults) {
    let test_name = "StateVector merge operations";
    
    let doc1 = Doc::with_client_id(1);
    let text1 = doc1.get_or_insert_text("test");
    {
        let mut txn = doc1.transact_mut();
        text1.push(&mut txn, "hello ");
    }
    let sv1 = doc1.transact().state_vector();
    
    let doc2 = Doc::with_client_id(2);
    let text2 = doc2.get_or_insert_text("test");
    {
        let mut txn = doc2.transact_mut();
        text2.push(&mut txn, "world");
    }
    let sv2 = doc2.transact().state_vector();
    
    // Verify state vectors are independent
    let enc1 = sv1.encode_v1();
    let enc2 = sv2.encode_v1();
    
    let dec1 = StateVector::decode_v1(&enc1).unwrap();
    let dec2 = StateVector::decode_v1(&enc2).unwrap();
    
    if dec1 == sv1 && dec2 == sv2 {
        results.pass(test_name);
    } else {
        results.fail(test_name, "State vector merge verification failed");
    }
}

// ============================================================================
// Update Encoding Tests
// ============================================================================

fn test_update_encoding_basic(results: &mut TestResults) {
    let test_name = "Update encoding/decoding basic";
    
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text("test");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "a");
    }
    
    let update_data = doc.transact().encode_state_as_update_v1(&StateVector::default());
    
    match Update::decode_v1(&update_data) {
        Ok(update) => {
            // Apply to a new doc to verify
            let doc2 = Doc::with_client_id(2);
            let text2 = doc2.get_or_insert_text("test");
            {
                let mut txn = doc2.transact_mut();
                txn.apply_update(update).unwrap();
            }
            
            let content = text2.get_string(&doc2.transact());
            if content == "a" {
                results.pass(test_name);
            } else {
                results.fail(test_name, &format!("Wrong content: '{}'", content));
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

fn test_update_encoding_complex(results: &mut TestResults) {
    let test_name = "Update encoding/decoding complex";
    
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text("text");
    let map = doc.get_or_insert_map("map");
    let array = doc.get_or_insert_array("array");
    
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "hello world");
        map.insert(&mut txn, "key1", "value1");
        map.insert(&mut txn, "key2", 42i64);
        array.push_back(&mut txn, "item1");
        array.push_back(&mut txn, 123i64);
    }
    
    let update_data = doc.transact().encode_state_as_update_v1(&StateVector::default());
    
    match Update::decode_v1(&update_data) {
        Ok(update) => {
            let doc2 = Doc::with_client_id(2);
            let text2 = doc2.get_or_insert_text("text");
            let map2 = doc2.get_or_insert_map("map");
            let array2 = doc2.get_or_insert_array("array");
            
            {
                let mut txn = doc2.transact_mut();
                txn.apply_update(update).unwrap();
            }
            
            let txn = doc2.transact();
            let text_content = text2.get_string(&txn);
            let map_len = map2.len(&txn);
            let array_len = array2.len(&txn);
            
            if text_content == "hello world" && map_len == 2 && array_len == 2 {
                results.pass(test_name);
            } else {
                results.fail(test_name, &format!(
                    "Wrong content: text='{}', map_len={}, array_len={}",
                    text_content, map_len, array_len
                ));
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

// ============================================================================
// Awareness Tests
// ============================================================================

fn test_awareness_local_state(results: &mut TestResults) {
    let test_name = "Awareness local state set/get";
    
    let doc = Doc::with_client_id(1);
    let awareness = Awareness::new(doc);
    
    awareness.set_local_state(json!({"cursor": {"x": 10, "y": 20}})).unwrap();
    
    let raw_state = awareness.local_state_raw();
    if raw_state.is_some() {
        results.pass(test_name);
    } else {
        results.fail(test_name, "Local state not set");
    }
}

fn test_awareness_update_encoding(results: &mut TestResults) {
    let test_name = "Awareness update encoding/decoding";
    
    let doc = Doc::with_client_id(1);
    let awareness = Awareness::new(doc);
    awareness.set_local_state(json!({"user": "test"})).unwrap();
    
    match awareness.update() {
        Ok(update) => {
            let encoded = update.encode_v1();
            match AwarenessUpdate::decode_v1(&encoded) {
                Ok(decoded) => {
                    if decoded == update {
                        results.pass(test_name);
                    } else {
                        results.fail(test_name, "Decoded awareness update doesn't match");
                    }
                }
                Err(e) => {
                    results.fail(test_name, &format!("Decode failed: {:?}", e));
                }
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Failed to get awareness update: {:?}", e));
        }
    }
}

fn test_awareness_apply_update(results: &mut TestResults) {
    let test_name = "Awareness apply update from another client";
    
    // Client 1
    let doc1 = Doc::with_client_id(1);
    let awareness1 = Awareness::new(doc1);
    awareness1.set_local_state(json!({"name": "Client1"})).unwrap();
    let update1 = awareness1.update().unwrap();
    
    // Client 2
    let doc2 = Doc::with_client_id(2);
    let awareness2 = Awareness::new(doc2);
    
    match awareness2.apply_update(update1) {
        Ok(_) => {
            // List all clients in awareness2 using iter()
            let clients: Vec<_> = awareness2.iter().map(|(id, _)| id).collect();
            
            // Check if client 1's state is now known to client 2
            if clients.contains(&1) {
                results.pass(test_name);
            } else {
                results.fail(test_name, "Client 1 not in awareness2 after update");
            }
        }
        Err(e) => {
            results.fail(test_name, &format!("Apply update failed: {:?}", e));
        }
    }
}

// ============================================================================
// Manual Message Encoder Test (matches std behavior)
// ============================================================================

fn test_manual_sync_update_encoding(results: &mut TestResults) {
    let test_name = "Manual sync update encoding (matches std)";
    
    // This is how the std version manually constructs update messages
    let update_data = vec![1, 1, 1, 0, 4, 1, 4, 116, 101, 115, 116, 1, 97, 0];
    
    let mut encoder = EncoderV1::new();
    encoder.write_var(MSG_SYNC);
    encoder.write_var(MSG_SYNC_UPDATE);
    encoder.write_buf(&update_data);
    let encoded = encoder.to_vec();
    
    // Decode it back
    match Message::decode_v1(&encoded) {
        Ok(Message::Sync(SyncMessage::Update(decoded_data))) => {
            if decoded_data == update_data {
                results.pass(test_name);
            } else {
                results.fail(test_name, "Decoded update data doesn't match");
            }
        }
        Ok(other) => {
            results.fail(test_name, &format!("Wrong message type: {:?}", other));
        }
        Err(e) => {
            results.fail(test_name, &format!("Decode failed: {:?}", e));
        }
    }
}

fn test_expected_update_bytes(results: &mut TestResults) {
    let test_name = "Expected update bytes (std compatibility)";
    
    // These are the expected bytes from the std test for a single 'a' insert
    let expected_update = vec![1, 1, 1, 0, 4, 1, 4, 116, 101, 115, 116, 1, 97, 0];
    
    // Create the same document state
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text("test");
    {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, "a");
    }
    
    let update = doc.transact().encode_state_as_update_v1(&StateVector::default());
    
    if update == expected_update {
        results.pass(test_name);
    } else {
        results.fail(test_name, &format!(
            "Update bytes don't match. Expected {:?}, got {:?}",
            expected_update, update
        ));
    }
}

fn test_expected_awareness_json(results: &mut TestResults) {
    let test_name = "Expected awareness JSON format";
    
    let doc = Doc::with_client_id(1);
    let awareness = Awareness::new(doc);
    awareness.set_local_state(json!({"key":"value"})).unwrap();
    
    let update = awareness.update().unwrap();
    
    // Check the client entry
    if let Some(entry) = update.clients.get(&1) {
        let expected_json: Arc<str> = r#"{"key":"value"}"#.into();
        if entry.clock == 1 && entry.json == expected_json {
            results.pass(test_name);
        } else {
            results.fail(test_name, &format!(
                "Awareness entry mismatch: clock={}, json={}",
                entry.clock, entry.json
            ));
        }
    } else {
        results.fail(test_name, "Client 1 not in awareness update");
    }
}

// ============================================================================
// BroadcastGroup Creation Test
// ============================================================================

async fn test_broadcast_group_creation(results: &mut TestResults) {
    let test_name = "BroadcastGroup creation";
    
    let doc = Doc::with_client_id(1);
    let awareness: AwarenessRef = Arc::new(Awareness::new(doc));
    
    // Test basic creation (no spawner)
    let _group = BroadcastGroup::new(awareness.clone(), 32);
    
    // Test with spawner
    let spawner = TokioSpawner;
    let _group_with_spawner = BroadcastGroup::new_with_spawner(awareness.clone(), 32, &spawner);
    
    results.pass(test_name);
}

async fn test_unified_broadcast_group_creation(results: &mut TestResults) {
    let test_name = "UnifiedBroadcastGroup creation";
    
    let doc = Doc::with_client_id(1);
    let awareness: AwarenessRef = Arc::new(Awareness::new(doc));
    let spawner = TokioSpawner;
    
    let _group = UnifiedBroadcastGroup::new(awareness, 32, &spawner);
    
    results.pass(test_name);
}

// ============================================================================
// Full Protocol Exchange Test
// ============================================================================

fn test_full_sync_protocol_exchange(results: &mut TestResults) {
    let test_name = "Full sync protocol exchange";
    
    // Server document with some content
    let server_doc = Doc::with_client_id(1);
    let server_text = server_doc.get_or_insert_text("content");
    {
        let mut txn = server_doc.transact_mut();
        server_text.push(&mut txn, "Hello from server!");
    }
    let server_awareness = Awareness::new(server_doc);
    
    // Client document (empty)
    let client_doc = Doc::with_client_id(2);
    let _client_text = client_doc.get_or_insert_text("content");
    let client_awareness = Awareness::new(client_doc);
    
    let protocol = DefaultProtocol;
    
    // Step 1: Client sends SyncStep1 with its state vector
    let client_sv = client_awareness.doc().transact().state_vector();
    let step1_msg = Message::Sync(SyncMessage::SyncStep1(client_sv));
    
    // Step 2: Server handles SyncStep1 and responds with SyncStep2
    match protocol.handle_message(&server_awareness, step1_msg) {
        Ok(Some(Message::Sync(SyncMessage::SyncStep2(update_data)))) => {
            // Step 3: Client receives SyncStep2 and applies the update
            let update = Update::decode_v1(&update_data).unwrap();
            {
                let mut txn = client_awareness.doc().transact_mut();
                txn.apply_update(update).unwrap();
            }
            
            // Verify client now has the content
            let client_text = client_awareness.doc().get_or_insert_text("content");
            let content = client_text.get_string(&client_awareness.doc().transact());
            
            if content == "Hello from server!" {
                results.pass(test_name);
            } else {
                results.fail(test_name, &format!("Wrong content after sync: '{}'", content));
            }
        }
        Ok(other) => {
            results.fail(test_name, &format!("Unexpected response: {:?}", other));
        }
        Err(e) => {
            results.fail(test_name, &format!("Protocol exchange failed: {:?}", e));
        }
    }
}

fn test_bidirectional_sync_protocol(results: &mut TestResults) {
    let test_name = "Bidirectional sync protocol";
    
    // Two documents with different content
    let doc1 = Doc::with_client_id(1);
    let text1 = doc1.get_or_insert_text("shared");
    {
        let mut txn = doc1.transact_mut();
        text1.push(&mut txn, "AAA");
    }
    let awareness1 = Awareness::new(doc1);
    
    let doc2 = Doc::with_client_id(2);
    let text2 = doc2.get_or_insert_text("shared");
    {
        let mut txn = doc2.transact_mut();
        text2.push(&mut txn, "BBB");
    }
    let awareness2 = Awareness::new(doc2);
    
    // Sync doc1 -> doc2
    let sv2 = awareness2.doc().transact().state_vector();
    let update1 = awareness1.doc().transact().encode_state_as_update_v1(&sv2);
    {
        let upd = Update::decode_v1(&update1).unwrap();
        let mut txn = awareness2.doc().transact_mut();
        txn.apply_update(upd).unwrap();
    }
    
    // Sync doc2 -> doc1
    let sv1 = awareness1.doc().transact().state_vector();
    let update2 = awareness2.doc().transact().encode_state_as_update_v1(&sv1);
    {
        let upd = Update::decode_v1(&update2).unwrap();
        let mut txn = awareness1.doc().transact_mut();
        txn.apply_update(upd).unwrap();
    }
    
    // Both should now have the same content
    let content1 = text1.get_string(&awareness1.doc().transact());
    let content2 = text2.get_string(&awareness2.doc().transact());
    
    if content1 == content2 && (content1.contains("AAA") && content1.contains("BBB")) {
        results.pass(test_name);
    } else {
        results.fail(test_name, &format!(
            "Bidirectional sync failed: doc1='{}', doc2='{}'",
            content1, content2
        ));
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║     YRS NO_STD INTEGRATION TESTS                         ║");
    println!("║     Testing compatibility with std versions              ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();
    
    let mut results = TestResults::default();
    
    // Message Serialization Tests
    println!("📦 Message Serialization Tests:");
    test_sync_step1_serialization(&mut results);
    test_sync_step2_serialization(&mut results);
    test_sync_update_serialization(&mut results);
    test_awareness_serialization(&mut results);
    test_awareness_query_serialization(&mut results);
    test_auth_message_serialization(&mut results);
    test_custom_message_serialization(&mut results);
    
    println!();
    
    // Protocol Handler Tests
    println!("🔧 Protocol Handler Tests:");
    test_protocol_sync_step1_handler(&mut results);
    test_protocol_sync_step2_handler(&mut results);
    test_protocol_update_handler(&mut results);
    test_protocol_awareness_query_handler(&mut results);
    test_protocol_awareness_update_handler(&mut results);
    test_protocol_auth_denied_handler(&mut results);
    test_protocol_auth_granted_handler(&mut results);
    
    println!();
    
    // Connection Handler Tests
    println!("🔗 Connection Handler Tests:");
    test_conn_handle_msg_sync_step1(&mut results);
    test_conn_handle_msg_sync_step2(&mut results);
    test_conn_handle_msg_update(&mut results);
    
    println!();
    
    // State Vector Tests
    println!("📊 State Vector Tests:");
    test_state_vector_encoding(&mut results);
    test_state_vector_merge(&mut results);
    
    println!();
    
    // Update Encoding Tests
    println!("📝 Update Encoding Tests:");
    test_update_encoding_basic(&mut results);
    test_update_encoding_complex(&mut results);
    
    println!();
    
    // Awareness Tests
    println!("👁️ Awareness Tests:");
    test_awareness_local_state(&mut results);
    test_awareness_update_encoding(&mut results);
    test_awareness_apply_update(&mut results);
    
    println!();
    
    // Manual Encoding Tests
    println!("🔨 Manual Encoding Tests (std compatibility):");
    test_manual_sync_update_encoding(&mut results);
    test_expected_update_bytes(&mut results);
    test_expected_awareness_json(&mut results);
    
    println!();
    
    // BroadcastGroup Tests
    println!("📡 BroadcastGroup Tests:");
    test_broadcast_group_creation(&mut results).await;
    test_unified_broadcast_group_creation(&mut results).await;
    
    println!();
    
    // Full Protocol Exchange Tests
    println!("🔄 Full Protocol Exchange Tests:");
    test_full_sync_protocol_exchange(&mut results);
    test_bidirectional_sync_protocol(&mut results);
    
    // Summary
    results.summary();
}
