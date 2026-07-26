use remote_workspace_protocol::*;
use serde_json::json;

fn round_trip(req: &Request) -> Request {
    let line = serde_json::to_string(req).unwrap();
    let back: Request = serde_json::from_str(&line).unwrap();
    assert_eq!(line, serde_json::to_string(&back).unwrap());
    back
}

#[test]
fn list_round_trips() {
    let req = Request {
        request_id: "r1".into(),
        body: RequestBody::List {
            path: "src".into(),
            offset: None,
            limit: None,
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(line, r#"{"request_id":"r1","op":"list","path":"src"}"#);
    round_trip(&req);
}

#[test]
fn read_round_trips() {
    let req = Request {
        request_id: "r2".into(),
        body: RequestBody::Read {
            path: "src/main.py".into(),
            offset: Some(0),
            limit: Some(65536),
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r2","op":"read","path":"src/main.py","offset":0,"limit":65536}"#
    );
    round_trip(&req);
}

#[test]
fn create_round_trips() {
    let req = Request {
        request_id: "r3".into(),
        body: RequestBody::Create {
            path: "src/new.py".into(),
            content: "print()\n".into(),
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r3","op":"create","path":"src/new.py","content":"print()\n"}"#
    );
    round_trip(&req);
}

#[test]
fn edit_round_trips() {
    let req = Request {
        request_id: "r3".into(),
        body: RequestBody::Edit {
            path: "src/main.py".into(),
            base_hash: "sha256:abc123".into(),
            old_text: "old".into(),
            new_text: "new".into(),
            replace_all: false,
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r3","op":"edit","path":"src/main.py","base_hash":"sha256:abc123","old_text":"old","new_text":"new","replace_all":false}"#
    );
    round_trip(&req);

    // replace_all defaults to false when omitted on the wire.
    let req: Request = serde_json::from_value(json!({
        "request_id": "r",
        "op": "edit",
        "path": "p",
        "base_hash": "sha256:a",
        "old_text": "x",
        "new_text": "y",
    }))
    .unwrap();
    match req.body {
        RequestBody::Edit { replace_all, .. } => assert!(!replace_all),
        _ => panic!("wrong body"),
    }
}

#[test]
fn exec_round_trips() {
    let req = Request {
        request_id: "r4".into(),
        body: RequestBody::Exec {
            argv: vec!["pytest".into(), "-q".into()],
            cwd: Some(".".into()),
            profile: Some("robot".into()),
            timeout_ms: Some(300000),
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r4","op":"exec","argv":["pytest","-q"],"cwd":".","profile":"robot","timeout_ms":300000}"#
    );
    round_trip(&req);
}

#[test]
fn delete_round_trips() {
    let req = Request {
        request_id: "r5".into(),
        body: RequestBody::Delete {
            path: "old.txt".into(),
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r5","op":"delete","path":"old.txt"}"#
    );
    round_trip(&req);
}

#[test]
fn result_message_serializes() {
    let msg = ServerMessage::Result {
        request_id: "r1".into(),
        result: ResultBody::Read(ReadResult {
            content: "...".into(),
            hash: Some("sha256:abc123".into()),
            truncated: false,
            next_offset: None,
        }),
    };
    let line = serde_json::to_string(&msg).unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["request_id"], "r1");
    assert_eq!(v["type"], "read");
    assert_eq!(v["content"], "...");
    assert_eq!(v["hash"], "sha256:abc123");
    assert_eq!(v["truncated"], false);
}

#[test]
fn error_message_serializes_with_hashes() {
    let msg = ServerMessage::Error {
        request_id: "r3".into(),
        error: ProtocolError::new(ErrorCode::StaleFile, "file changed")
            .with_hashes("sha256:abc123".into(), "sha256:def456".into()),
    };
    let line = serde_json::to_string(&msg).unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["request_id"], "r3");
    assert_eq!(v["code"], "STALE_FILE");
    assert_eq!(v["expected_hash"], "sha256:abc123");
    assert_eq!(v["actual_hash"], "sha256:def456");
}

#[test]
fn exec_result_message() {
    let result = ServerMessage::Result {
        request_id: "r4".into(),
        result: ResultBody::Exec(ExecResult {
            operation_id: "op-43".into(),
            termination: ExecTermination::Exited { code: 0 },
            duration_ms: 12,
            drain_timed_out: false,
            stdout: ExecOutput {
                prefix: "collecting tests...\n".into(),
                suffix: String::new(),
                total_bytes: 20,
                omitted_bytes: 0,
            },
            stderr: ExecOutput::default(),
        }),
    };
    let line = serde_json::to_string(&result).unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["type"], "exec");
    assert_eq!(v["termination"]["kind"], "exited");
    assert_eq!(v["termination"]["code"], 0);
    assert_eq!(v["operation_id"], "op-43");
    assert_eq!(v["stdout"]["prefix"], "collecting tests...\n");
}

#[test]
fn unknown_op_returns_invalid_request_on_deserialize() {
    let bad = r#"{"request_id":"r","op":"frobnicate","path":"x"}"#;
    let err = serde_json::from_str::<Request>(bad);
    assert!(err.is_err());
}

#[test]
fn mutation_result_message_keeps_legacy_wire_tag() {
    let msg = ServerMessage::Result {
        request_id: "r3".into(),
        result: ResultBody::Mutation(MutationResult {
            operation_id: "op-42".into(),
            old_hash: Some("sha256:abc123".into()),
            new_hash: "sha256:def456".into(),
        }),
    };
    let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&msg).unwrap()).unwrap();
    assert_eq!(v["operation_id"], "op-42");
    assert_eq!(v["old_hash"], "sha256:abc123");
    assert_eq!(v["new_hash"], "sha256:def456");
    // Tag must stay "write" so request logs recorded before the create/edit
    // protocol still deserialize.
    assert_eq!(v["type"], "write");
}

#[test]
fn legacy_records_and_results_still_deserialize() {
    // Operation logs written by pre-create/edit servers contain kinds "write"
    // and "patch" and exec records without drain_timed_out.
    let old_fs = r#"{"record_kind":"fs","operation_id":"op-1","request_id":"r","kind":"patch","after_hash":"sha256:z","path":"p","timestamp_ms":1}"#;
    let rec: AnyOperationRecord = serde_json::from_str(old_fs).unwrap();
    match rec {
        AnyOperationRecord::Fs(f) => assert_eq!(f.kind, OperationKind::Patch),
        _ => panic!("wrong record"),
    }
    let old_write = r#"{"record_kind":"fs","operation_id":"op-2","request_id":"r","kind":"write","after_hash":"sha256:z","path":"p","timestamp_ms":1}"#;
    assert!(serde_json::from_str::<AnyOperationRecord>(old_write).is_ok());

    let old_exec_result = r#"{"request_id":"r","type":"exec","operation_id":"op-3","termination":{"kind":"exited","code":0},"duration_ms":1,"stdout":{"prefix":"","suffix":"","total_bytes":0,"omitted_bytes":0},"stderr":{"prefix":"","suffix":"","total_bytes":0,"omitted_bytes":0}}"#;
    let msg: ServerMessage = serde_json::from_str(old_exec_result).unwrap();
    match msg {
        ServerMessage::Result {
            result: ResultBody::Exec(e),
            ..
        } => assert!(!e.drain_timed_out),
        _ => panic!("wrong message"),
    }
}

#[test]
fn enum_snake_case_tags() {
    // Ensure tag serialization stays lowercase snake_case as designed.
    let op = RequestBody::Stat { path: "x".into() };
    let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&op).unwrap()).unwrap();
    assert_eq!(v["op"], "stat");

    let op = RequestBody::OperationGet {
        operation_id: "op-1".into(),
    };
    let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&op).unwrap()).unwrap();
    assert_eq!(v["op"], "operation_get");
}

#[test]
fn json_literal_decodes_as_request() {
    let req: Request = serde_json::from_value(json!({
        "request_id": "r9",
        "op": "exec",
        "argv": ["echo", "hi"],
    }))
    .unwrap();
    match req.body {
        RequestBody::Exec {
            argv,
            cwd,
            profile,
            timeout_ms,
        } => {
            assert_eq!(argv, vec!["echo".to_string(), "hi".to_string()]);
            assert!(cwd.is_none());
            assert!(profile.is_none());
            assert!(timeout_ms.is_none());
        }
        _ => panic!("wrong body"),
    }
}

#[test]
fn every_result_variant_round_trips_through_server_message() {
    // Regression guard: ServerMessage::Result flattens both the envelope
    // request_id and the ResultBody. Any ResultBody field named request_id (or
    // any other duplicate) breaks serialization. Build each variant and verify
    // it round-trips.
    let cases: Vec<ResultBody> = vec![
        ResultBody::List(ListResult {
            entries: vec![],
            next_offset: None,
        }),
        ResultBody::Stat {
            stat: FileEntry {
                path: "x".into(),
                kind: ListKind::File,
                size: 1,
                hash: None,
                mode: None,
            },
        },
        ResultBody::Read(ReadResult {
            content: "c".into(),
            hash: None,
            truncated: false,
            next_offset: None,
        }),
        ResultBody::Mutation(MutationResult {
            operation_id: "op-1".into(),
            old_hash: None,
            new_hash: "sha256:x".into(),
        }),
        ResultBody::Exec(ExecResult {
            operation_id: "op-2".into(),
            termination: ExecTermination::Exited { code: 0 },
            duration_ms: 1,
            drain_timed_out: false,
            stdout: ExecOutput::default(),
            stderr: ExecOutput::default(),
        }),
        ResultBody::Undo(UndoResult {
            operation_id: "op-3".into(),
            restored_hash: None,
            new_hash: "sha256:y".into(),
        }),
        ResultBody::History { operations: vec![] },
        ResultBody::Operation(OperationDetails {
            record: remote_workspace_protocol::AnyOperationRecord::Fs(
                remote_workspace_protocol::FsOperationRecord {
                    operation_id: "op-4".into(),
                    request_id: "r".into(),
                    kind: remote_workspace_protocol::OperationKind::Create,
                    path: "p".into(),
                    before_hash: None,
                    after_hash: "sha256:z".into(),
                    timestamp_ms: 1,
                },
            ),
        }),
        ResultBody::RequestStatus(RequestStatusResult {
            target: "r".into(),
            status: RequestStatus::Done,
            error: None,
        }),
    ];
    for body in cases {
        let msg = ServerMessage::Result {
            request_id: "envelope".into(),
            result: body,
        };
        let s = serde_json::to_string(&msg).expect("must serialize without collision");
        let back: ServerMessage = serde_json::from_str(&s).expect("must deserialize round-trip");
        assert_eq!(s, serde_json::to_string(&back).unwrap());
    }
}

#[test]
fn transfer_requests_round_trip() {
    let req = Request {
        request_id: "r10".into(),
        body: RequestBody::UploadPrepare {
            path: "@scratch/model.pt".into(),
            overwrite: false,
        },
    };
    let line = serde_json::to_string(&req).unwrap();
    assert_eq!(
        line,
        r#"{"request_id":"r10","op":"upload_prepare","path":"@scratch/model.pt","overwrite":false}"#
    );
    round_trip(&req);

    let req = Request {
        request_id: "r11".into(),
        body: RequestBody::UploadCommit {
            transfer_id: "xfer-1".into(),
            size: 42,
            sha256: "sha256:abc".into(),
            duration_ms: 7,
        },
    };
    round_trip(&req);

    let req = Request {
        request_id: "r12".into(),
        body: RequestBody::UploadAbort {
            transfer_id: "xfer-1".into(),
        },
    };
    round_trip(&req);

    let req = Request {
        request_id: "r13".into(),
        body: RequestBody::DownloadRecord {
            path: "logs/out.bin".into(),
            size: 42,
            sha256: "sha256:abc".into(),
            duration_ms: 7,
        },
    };
    round_trip(&req);
}

#[test]
fn transfer_results_round_trip_through_server_message() {
    let cases: Vec<ResultBody> = vec![
        ResultBody::UploadPrepare(UploadPrepareResult {
            transfer_id: "xfer-1".into(),
            staging_path: "/ws/.f.part".into(),
        }),
        ResultBody::UploadAbort {
            transfer_id: "xfer-1".into(),
        },
        ResultBody::Transfer(TransferResult {
            operation_id: "op-9".into(),
            direction: TransferDirection::Upload,
            path: "@scratch/model.pt".into(),
            size: 42,
            sha256: "sha256:abc".into(),
            duration_ms: 7,
        }),
    ];
    for body in cases {
        let msg = ServerMessage::Result {
            request_id: "envelope".into(),
            result: body,
        };
        let s = serde_json::to_string(&msg).expect("must serialize without collision");
        let back: ServerMessage = serde_json::from_str(&s).expect("must deserialize round-trip");
        assert_eq!(s, serde_json::to_string(&back).unwrap());
    }
    let v: serde_json::Value = serde_json::to_value(ResultBody::Transfer(TransferResult {
        operation_id: "op-9".into(),
        direction: TransferDirection::Download,
        path: "p".into(),
        size: 1,
        sha256: "sha256:x".into(),
        duration_ms: 2,
    }))
    .unwrap();
    assert_eq!(v["type"], "transfer");
    assert_eq!(v["direction"], "download");
}

#[test]
fn transfer_record_round_trips_as_any_operation_record() {
    let record = AnyOperationRecord::Transfer(TransferOperationRecord {
        operation_id: "op-5".into(),
        request_id: "r".into(),
        direction: TransferDirection::Upload,
        path: "data/x.bin".into(),
        size: 10,
        sha256: "sha256:y".into(),
        duration_ms: 3,
        timestamp_ms: 4,
    });
    assert!(record.is_committed());
    assert_eq!(record.operation_id(), "op-5");
    let s = serde_json::to_string(&record).unwrap();
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["record_kind"], "transfer");
    let back: AnyOperationRecord = serde_json::from_str(&s).unwrap();
    assert_eq!(s, serde_json::to_string(&back).unwrap());
}
