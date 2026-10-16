import asyncio
import os
import shutil
import tempfile
import time
import pytest
from protocol import Message, MessageType, CommandType, CommandPayloads, ResponsePayloads
from server import TestingProxy
from client import KaggleNotebookRunner, ManagedProcess
from manager import InferenceClientManager


def test_protocol_from_json_extra_fields():
    msg = Message(message_type=MessageType.COMMAND, payload={"command": "test"})
    json_str = msg.to_json()
    import json
    data = json.loads(json_str)
    data["extra_unknown_field"] = "unexpected_value"
    modified_json = json.dumps(data)
    
    parsed = Message.from_json(modified_json)
    assert parsed.message_type == MessageType.COMMAND
    assert parsed.payload == {"command": "test"}


def test_process_start_restart_string_cmd():
    with tempfile.TemporaryDirectory() as tmpdir:
        runner = KaggleNotebookRunner(proxy_url="ws://localhost:12345/notebook")
        runner._cwd = tmpdir
        
        # Test process_start with string shell command
        cmd_str = f"cd {tmpdir} && echo 'hello world' > file.txt && cat file.txt"
        msg = Message(message_type=MessageType.COMMAND, payload=CommandPayloads.process_start(cmd_str))
        
        start_res = runner._cmd_process_start(msg)
        assert start_res["success"] is True
        proc_info = start_res["process"]
        pid_id = proc_info["process_id"]
        
        # Wait for process using process_wait fix
        wait_msg = Message(message_type=MessageType.COMMAND, payload=CommandPayloads.process_wait(pid_id, timeout=5.0))
        wait_res = runner._cmd_process_wait(wait_msg)
        assert wait_res["success"] is True
        assert wait_res["process"]["status"] in ("completed", "running")
        
        # Check logs
        logs_msg = Message(message_type=MessageType.COMMAND, payload=CommandPayloads.process_logs(pid_id))
        logs_res = runner._cmd_process_logs(logs_msg)
        assert logs_res["success"] is True
        assert "hello world" in logs_res["stdout"]
        
        # Test process_restart with string shell command
        restart_msg = Message(message_type=MessageType.COMMAND, payload=CommandPayloads.process_restart(pid_id))
        restart_res = runner._cmd_process_restart(restart_msg)
        assert restart_res["success"] is True
        
        wait_msg2 = Message(message_type=MessageType.COMMAND, payload=CommandPayloads.process_wait(pid_id, timeout=5.0))
        wait_res2 = runner._cmd_process_wait(wait_msg2)
        assert wait_res2["success"] is True


def test_shell_stream_string_cmd():
    with tempfile.TemporaryDirectory() as tmpdir:
        runner = KaggleNotebookRunner(proxy_url="ws://localhost:12345/notebook")
        runner._cwd = tmpdir
        
        streamed_lines = []
        def mock_stream(msg, payload, **kwargs):
            if payload.get("kind") == "shell_line":
                streamed_lines.append(payload.get("line"))

        runner._stream = mock_stream
        
        msg = Message(message_type=MessageType.COMMAND, payload={
            "command": "shell_exec",
            "shell_command": f"cd {tmpdir} && echo line1 && echo line2",
            "stream": True,
            "timeout": 10.0,
        })
        
        res = runner._cmd_shell_exec(msg)
        assert res["success"] is True
        assert res["streamed"] is True
        assert "line1" in res["stdout"]
        assert "line2" in res["stdout"]
        assert "line1" in streamed_lines
        assert "line2" in streamed_lines


def test_no_duplicate_response_on_handler_returning_none():
    runner = KaggleNotebookRunner(proxy_url="ws://localhost:12345/notebook")
    runner.ws = "mock_ws"
    replies = []
    def mock_reply(msg, payload):
        replies.append(payload)
    runner._reply = mock_reply
    
    # Send a command whose handler returns None (e.g. notebook_shutdown)
    msg = Message(message_type=MessageType.COMMAND, payload=CommandPayloads.notebook_shutdown())
    runner._handle_command(msg)
    
    # Should only have sent the reply inside notebook_shutdown once, not an extra None reply
    assert len(replies) == 1
    assert replies[0]["shutdown"] is True


@pytest.mark.asyncio
async def test_full_system_integration():
    port = 18766
    proxy = TestingProxy(host="127.0.0.1", port=port, heartbeat_timeout=5.0)
    
    server_task = asyncio.create_task(proxy.serve_forever())
    await asyncio.sleep(0.2)
    
    runner = KaggleNotebookRunner(
        proxy_url=f"ws://127.0.0.1:{port}/notebook",
        notebook_id="test-nb-1",
        heartbeat_interval=0.5,
        reconnect_delay=1.0,
        max_heartbeat_misses=3,
    )
    
    import threading
    runner_thread = threading.Thread(target=runner.run, daemon=True)
    runner_thread.start()
    
    await asyncio.sleep(0.5)
    
    mgr = InferenceClientManager(proxy_url=f"ws://127.0.0.1:{port}/manager")
    
    try:
        # Test 1: Proxy list
        proxy_list = await mgr.request("proxy_list")
        assert proxy_list["success"] is True
        assert len(proxy_list["notebooks"]) == 1
        assert proxy_list["notebooks"][0]["notebook_id"] == "test-nb-1"
        assert proxy_list["notebooks"][0]["status"] == "connected"
        
        # Test 2: Long running shell command (Requirement 1 test)
        # Should take 2.0 seconds while heartbeats (interval=0.5s) continue on receive loop
        start_t = time.time()
        res = await mgr.request("shell_exec", notebook_id="test-nb-1", extra={
            "shell_command": "sleep 2.0 && echo done_sleeping",
            "timeout": 10.0,
        })
        elapsed = time.time() - start_t
        assert res["success"] is True
        assert "done_sleeping" in res["stdout"]
        assert elapsed >= 1.8
        
        # Verify runner did NOT disconnect during long command execution
        assert runner.connected is True
        assert runner._consecutive_hb_misses == 0
        
        # Test 3: String shell command execution (Requirement 2 test)
        res_cmd = await mgr.request("shell_exec", notebook_id="test-nb-1", extra={
            "shell_command": "cd /tmp && pwd && python3 -c 'print(\"python_ok\")'",
        })
        assert res_cmd["success"] is True
        assert "/tmp" in res_cmd["stdout"]
        assert "python_ok" in res_cmd["stdout"]
        
        # Test 4: Process start with string shell command
        p_start = await mgr.request("process_start", notebook_id="test-nb-1", extra={
            "shell_command": "cd /tmp && echo 'bg_proc_output' && sleep 0.5",
        })
        assert p_start["success"] is True
        pid = p_start["process"]["process_id"]
        
        # Process wait
        p_wait = await mgr.request("process_wait", notebook_id="test-nb-1", extra={
            "process_id": pid,
            "timeout": 5.0,
        })
        assert p_wait["success"] is True
        
        p_logs = await mgr.request("process_logs", notebook_id="test-nb-1", extra={
            "process_id": pid,
        })
        assert p_logs["success"] is True
        assert "bg_proc_output" in p_logs["stdout"]

    finally:
        runner.shutdown()
        proxy._request_shutdown()
        server_task.cancel()
        try:
            await server_task
        except (asyncio.CancelledError, Exception):
            pass


@pytest.mark.asyncio
async def test_reconnect_command_queueing():
    port = 18767
    proxy = TestingProxy(host="127.0.0.1", port=port, heartbeat_timeout=5.0)
    server_task = asyncio.create_task(proxy.serve_forever())
    await asyncio.sleep(0.2)
    
    # 1. Connect first runner
    runner1 = KaggleNotebookRunner(
        proxy_url=f"ws://127.0.0.1:{port}/notebook",
        notebook_id="queue-nb",
        heartbeat_interval=0.5,
    )
    import threading
    t1 = threading.Thread(target=runner1.run, daemon=True)
    t1.start()
    await asyncio.sleep(0.4)
    
    assert runner1.notebook_id == "queue-nb"
    secret = runner1.secret
    
    # Disconnect runner1
    runner1.shutdown()
    await asyncio.sleep(0.3)
    
    # 2. Manager sends command while runner is offline
    mgr = InferenceClientManager(proxy_url=f"ws://127.0.0.1:{port}/manager")
    
    async def issue_cmd():
        return await mgr.request("pwd", notebook_id="queue-nb", timeout=5.0)

    cmd_task = asyncio.create_task(issue_cmd())
    await asyncio.sleep(0.3)
    
    # 3. Connect runner2 with same notebook_id and secret (resuming connection)
    runner2 = KaggleNotebookRunner(
        proxy_url=f"ws://127.0.0.1:{port}/notebook",
        notebook_id="queue-nb",
        heartbeat_interval=0.5,
    )
    runner2.secret = secret
    t2 = threading.Thread(target=runner2.run, daemon=True)
    t2.start()
    
    # 4. Wait for command result
    res = await cmd_task
    assert res["success"] is True
    assert "queued" in res
    assert "cwd" in res
    
    runner2.shutdown()
    proxy._request_shutdown()
    server_task.cancel()
    try:
        await server_task
    except (asyncio.CancelledError, Exception):
        pass


def test_malformed_message_parsing_errors():
    import json
    # Bad JSON syntax -> JSONDecodeError
    with pytest.raises((json.JSONDecodeError, ValueError, KeyError, TypeError)):
        Message.from_json("invalid json")

    # Non-dict JSON -> ValueError
    with pytest.raises((json.JSONDecodeError, ValueError, KeyError, TypeError)):
        Message.from_json("12345")

    # Missing message_type key -> KeyError
    with pytest.raises((json.JSONDecodeError, ValueError, KeyError, TypeError)):
        Message.from_json(json.dumps({"message_id": "1"}))

    # Invalid message_type enum -> ValueError
    with pytest.raises((json.JSONDecodeError, ValueError, KeyError, TypeError)):
        Message.from_json(json.dumps({"message_type": "invalid_type"}))


@pytest.mark.asyncio
async def test_blocking_command_does_not_stall_others():
    port = 18769
    proxy = TestingProxy(host="127.0.0.1", port=port, heartbeat_timeout=5.0)
    server_task = asyncio.create_task(proxy.serve_forever())
    await asyncio.sleep(0.2)

    runner = KaggleNotebookRunner(
        proxy_url=f"ws://127.0.0.1:{port}/notebook",
        notebook_id="block-nb",
        heartbeat_interval=0.5,
    )
    import threading
    t = threading.Thread(target=runner.run, daemon=True)
    t.start()
    await asyncio.sleep(0.5)

    mgr = InferenceClientManager(proxy_url=f"ws://127.0.0.1:{port}/manager")
    try:
        # A command that blocks for far longer than the manager would tolerate.
        async def blocking():
            return await mgr.request("shell_exec", notebook_id="block-nb", extra={
                "shell_command": "sleep 30 && echo done",
            }, timeout=5.0)

        block_task = asyncio.create_task(blocking())
        await asyncio.sleep(0.4)

        # A second command must still be served promptly despite the first
        # command's worker thread being blocked.
        start_t = time.time()
        res = await mgr.request("pwd", notebook_id="block-nb", timeout=5.0)
        elapsed = time.time() - start_t
        assert res["success"] is True
        assert "cwd" in res
        assert elapsed < 3.0

        block_task.cancel()
        try:
            await block_task
        except (asyncio.CancelledError, Exception):
            pass

        # Clean up the still-running remote job.
        jobs = await mgr.request("job_list", notebook_id="block-nb", timeout=5.0)
        for job in jobs.get("jobs", []):
            if job.get("status") == "running":
                await mgr.request("job_terminate", notebook_id="block-nb", extra={
                    "job_id": job["process_id"],
                }, timeout=5.0)
    finally:
        runner.shutdown()
        proxy._request_shutdown()
        server_task.cancel()
        try:
            await server_task
        except (asyncio.CancelledError, Exception):
            pass


@pytest.mark.asyncio
async def test_shell_job_returns_id_and_output_fetchable():
    port = 18770
    proxy = TestingProxy(host="127.0.0.1", port=port, heartbeat_timeout=5.0)
    server_task = asyncio.create_task(proxy.serve_forever())
    await asyncio.sleep(0.2)

    runner = KaggleNotebookRunner(
        proxy_url=f"ws://127.0.0.1:{port}/notebook",
        notebook_id="job-nb",
        heartbeat_interval=0.5,
    )
    import threading
    t = threading.Thread(target=runner.run, daemon=True)
    t.start()
    await asyncio.sleep(0.5)

    mgr = InferenceClientManager(proxy_url=f"ws://127.0.0.1:{port}/manager")
    try:
        res = await mgr.request("shell_exec", notebook_id="job-nb", extra={
            "shell_command": "sleep 1.5 && echo job_output_marker",
            "wait": 0.5,
        }, timeout=5.0)
        assert res["success"] is True
        assert res.get("status") == "running"
        job_id = res["job_id"]
        assert job_id

        jobs = await mgr.request("job_list", notebook_id="job-nb", timeout=5.0)
        assert jobs["success"] is True
        assert any(j["process_id"] == job_id for j in jobs["jobs"])

        waited = await mgr.request("job_wait", notebook_id="job-nb", extra={
            "job_id": job_id, "timeout": 5.0,
        }, timeout=8.0)
        assert waited["success"] is True
        assert "job_output_marker" in waited["stdout"]

        logs = await mgr.request("job_logs", notebook_id="job-nb", extra={
            "job_id": job_id,
        }, timeout=5.0)
        assert logs["success"] is True
        assert "job_output_marker" in logs["stdout"]
    finally:
        runner.shutdown()
        proxy._request_shutdown()
        server_task.cancel()
        try:
            await server_task
        except (asyncio.CancelledError, Exception):
            pass


@pytest.mark.asyncio
async def test_server_pending_routes_ttl_cleanup():
    proxy = TestingProxy(host="127.0.0.1", port=18768)
    proxy.managers["mgr-1"] = "mock_ws"
    proxy.pending["corr-1"] = ("mgr-1", time.time() - 400.0)  # expired
    proxy.pending["corr-2"] = ("mgr-1", time.time())        # active
    proxy.pending["corr-3"] = ("dead-mgr", time.time())     # disconnected mgr

    await proxy._gc_pending(ttl=300.0)
    assert "corr-1" not in proxy.pending
    assert "corr-3" not in proxy.pending
    assert "corr-2" in proxy.pending


def test_reconnect_backoff_interruptible():
    runner = KaggleNotebookRunner(
        proxy_url="ws://127.0.0.1:59999/notebook",
        reconnect_delay=10.0,
    )
    import threading
    t = threading.Thread(target=runner.run, daemon=True)
    start_t = time.time()
    t.start()
    time.sleep(0.2)
    runner.shutdown()
    t.join(timeout=2.0)
    elapsed = time.time() - start_t
    assert not t.is_alive()
    assert elapsed < 3.0


def test_file_delete_directory_non_recursive_safety():
    with tempfile.TemporaryDirectory() as tmpdir:
        runner = KaggleNotebookRunner(proxy_url="ws://localhost:12345/notebook")
        runner._cwd = tmpdir
        subdir = os.path.join(tmpdir, "subdir")
        os.makedirs(subdir)
        with open(os.path.join(subdir, "file.txt"), "w") as f:
            f.write("content")

        # Non-recursive delete on non-empty directory should fail safely without removing files
        msg = Message(message_type=MessageType.COMMAND, payload={"remote_path": subdir, "recursive": False})
        with pytest.raises(OSError):
            runner._cmd_file_delete(msg)
        assert os.path.exists(os.path.join(subdir, "file.txt"))

        # Recursive delete should succeed
        msg_rec = Message(message_type=MessageType.COMMAND, payload={"remote_path": subdir, "recursive": True})
        res = runner._cmd_file_delete(msg_rec)
        assert res["success"] is True
        assert not os.path.exists(subdir)


def test_python_exec_persistent_namespace():
    runner = KaggleNotebookRunner(proxy_url="ws://localhost:12345/notebook")
    # Step 1: define a variable and function
    msg1 = Message(message_type=MessageType.COMMAND, payload={"code": "x = 42\ndef get_val(): return x * 2"})
    res1 = runner._cmd_python_exec(msg1)
    assert res1["success"] is True

    # Step 2: call the function and check persistent namespace
    msg2 = Message(message_type=MessageType.COMMAND, payload={"code": "get_val()"})
    res2 = runner._cmd_python_exec(msg2)
    assert res2["success"] is True
    assert res2["result"] == 84


def test_env_management_and_overlay():
    runner = KaggleNotebookRunner(proxy_url="ws://localhost:12345/notebook")
    # env_set
    set_msg = Message(message_type=MessageType.COMMAND, payload={"key": "REMOTE_TEST_VAR", "value": "magic_123"})
    runner._cmd_env_set(set_msg)

    # env_get
    get_msg = Message(message_type=MessageType.COMMAND, payload={"key": "REMOTE_TEST_VAR"})
    res_get = runner._cmd_env_get(get_msg)
    assert res_get["value"] == "magic_123"

    # env_list
    list_msg = Message(message_type=MessageType.COMMAND, payload={})
    res_list = runner._cmd_env_list(list_msg)
    assert res_list["overlay"]["REMOTE_TEST_VAR"] == "magic_123"

    # env_unset
    unset_msg = Message(message_type=MessageType.COMMAND, payload={"key": "REMOTE_TEST_VAR"})
    runner._cmd_env_unset(unset_msg)
    res_get2 = runner._cmd_env_get(get_msg)
    assert res_get2["value"] is None


def test_cli_parser_all_subcommands():
    from manager import build_parser, _command_mapping
    parser = build_parser()

    # Test parser accepts <subcommand> <notebook_id>
    args = parser.parse_args(["shell", "nb-1", "echo hi", "--timeout", "10"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "shell_exec"
    assert nb == "nb-1"
    assert extra["shell_command"] == "echo hi"

    args = parser.parse_args(["push", "nb-1", "local.txt", "remote.txt"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "__push__"
    assert nb == "nb-1"

    args = parser.parse_args(["pull", "nb-1", "remote.txt", "local.txt"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "__pull__"
    assert nb == "nb-1"

    args = parser.parse_args(["ls", "nb-1", "/tmp", "-r"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "file_list"
    assert extra["recursive"] is True

    args = parser.parse_args(["cp", "nb-1", "src.txt", "dst.txt"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "file_copy"

    args = parser.parse_args(["mv", "nb-1", "src.txt", "dst.txt"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "file_move"

    args = parser.parse_args(["eval", "nb-1", "1 + 1"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "python_exec"
    assert extra["code"] == "1 + 1"

    args = parser.parse_args(["restart", "nb-1", "proc-123"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "process_restart"

    args = parser.parse_args(["shutdown", "nb-1"])
    cmd, nb, extra = _command_mapping(args, None)
    assert cmd == "notebook_shutdown"


@pytest.mark.asyncio
async def test_full_push_pull_chunked_transfer():
    port = 18771
    proxy = TestingProxy(host="127.0.0.1", port=port, heartbeat_timeout=5.0)
    server_task = asyncio.create_task(proxy.serve_forever())
    await asyncio.sleep(0.2)

    with tempfile.TemporaryDirectory() as tmp_remote, tempfile.TemporaryDirectory() as tmp_local:
        runner = KaggleNotebookRunner(
            proxy_url=f"ws://127.0.0.1:{port}/notebook",
            notebook_id="file-nb",
            heartbeat_interval=0.5,
        )
        runner._cwd = tmp_remote
        import threading
        t = threading.Thread(target=runner.run, daemon=True)
        t.start()
        await asyncio.sleep(0.4)

        mgr = InferenceClientManager(proxy_url=f"ws://127.0.0.1:{port}/manager")
        try:
            # 1. Create a binary file locally (1.5 MB to test multi-chunk transfer)
            local_file = os.path.join(tmp_local, "test_data.bin")
            content = os.urandom(1500 * 1024)
            with open(local_file, "wb") as f:
                f.write(content)
            import hashlib
            orig_hash = hashlib.sha256(content).hexdigest()

            # 2. Push to remote runner
            push_res = await mgr.push("file-nb", local_file, "remote_data.bin")
            assert push_res["success"] is True
            assert push_res["bytes"] == len(content)

            # 3. Pull back to local directory
            downloaded_file = os.path.join(tmp_local, "downloaded.bin")
            pull_res = await mgr.pull("file-nb", "remote_data.bin", downloaded_file)
            assert pull_res["success"] is True
            assert pull_res["bytes"] == len(content)
            assert pull_res["sha256"] == orig_hash
            assert pull_res["integrity_ok"] is True

            with open(downloaded_file, "rb") as f:
                downloaded_content = f.read()
            assert downloaded_content == content
        finally:
            runner.shutdown()
            proxy._request_shutdown()
            server_task.cancel()
            try:
                await server_task
            except (asyncio.CancelledError, Exception):
                pass


