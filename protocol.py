"""
Shared protocol definitions for the remote testing framework.
Defines message types, commands, and data structures used across all components.
"""

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Dict, List, Union


class MessageType(Enum):
    """Message types for communication between components."""
    # Connection management
    REGISTER = "register"
    REGISTER_ACK = "register_ack"
    HEARTBEAT = "heartbeat"
    HEARTBEAT_ACK = "heartbeat_ack"
    DISCONNECT = "disconnect"
    MANAGER_HELLO = "manager_hello"

    # Command execution
    COMMAND = "command"
    COMMAND_RESPONSE = "command_response"
    COMMAND_STREAM = "command_stream"
    COMMAND_QUEUED = "command_queued"

    # Notebook management
    NOTEBOOK_LIST = "notebook_list"
    NOTEBOOK_INFO = "notebook_info"
    NOTEBOOK_STATUS = "notebook_status"

    # Error handling
    ERROR = "error"


COMMAND_CHUNK_SIZE = 1024 * 1024


class CommandType(Enum):
    """Command types that can be executed on notebook runners."""
    # File operations
    FILE_UPLOAD = "file_upload"
    FILE_DOWNLOAD = "file_download"
    FILE_DELETE = "file_delete"
    FILE_MOVE = "file_move"
    FILE_COPY = "file_copy"
    FILE_LIST = "file_list"
    FILE_MKDIR = "file_mkdir"
    FILE_RMDIR = "file_rmdir"
    FILE_EXISTS = "file_exists"
    FILE_STAT = "file_stat"
    
    # Shell execution
    SHELL_EXEC = "shell_exec"
    SHELL_EXEC_ASYNC = "shell_exec_async"
    
    # Process management
    PROCESS_START = "process_start"
    PROCESS_TERMINATE = "process_terminate"
    PROCESS_LIST = "process_list"
    PROCESS_LOGS = "process_logs"
    PROCESS_WAIT = "process_wait"
    
    # Environment variables
    ENV_LIST = "env_list"
    ENV_GET = "env_get"
    ENV_SET = "env_set"
    ENV_UNSET = "env_unset"
    
    # Python execution
    PYTHON_EXEC = "python_exec"
    
    # System information
    SYSTEM_INFO = "system_info"
    PWD = "pwd"
    CD = "cd"

    # Notebook control
    NOTEBOOK_PING = "notebook_ping"
    NOTEBOOK_RESTART = "notebook_restart"
    NOTEBOOK_SHUTDOWN = "notebook_shutdown"

    # Proxy-level (handled by testing_proxy.py, not forwarded to runners)
    PROXY_LIST = "proxy_list"
    PROXY_INFO = "proxy_info"
    PROXY_HEARTBEAT = "proxy_heartbeat"
    PROXY_STATS = "proxy_stats"


class ProcessStatus(Enum):
    """Status of a managed process."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TERMINATED = "terminated"
    UNKNOWN = "unknown"


@dataclass
class Message:
    """Base message structure for all communication."""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    message_type: MessageType = MessageType.COMMAND
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    
    def to_json(self) -> str:
        """Serialize message to JSON."""
        data = asdict(self)
        data['message_type'] = self.message_type.value
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Deserialize message from JSON."""
        data = json.loads(json_str)
        data['message_type'] = MessageType(data['message_type'])
        return cls(**data)
    
    @classmethod
    def create_response(cls, request: 'Message', payload: Dict[str, Any], 
                       message_type: MessageType = MessageType.COMMAND_RESPONSE) -> 'Message':
        """Create a response message correlated to a request."""
        return cls(
            message_type=message_type,
            correlation_id=request.message_id,
            payload=payload
        )


@dataclass
class NotebookInfo:
    """Information about a connected notebook runner."""
    notebook_id: str
    hostname: str
    python_version: str
    os_info: str
    cuda_version: Optional[str] = None
    gpu_info: List[Dict[str, Any]] = field(default_factory=list)
    vram_total: Optional[int] = None
    vram_available: Optional[int] = None
    ram_total: Optional[int] = None
    ram_available: Optional[int] = None
    disk_total: Optional[int] = None
    disk_available: Optional[int] = None
    cwd: str = "/kaggle/working"
    uptime: float = 0.0
    installed_packages: List[str] = field(default_factory=list)
    connected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    last_heartbeat: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    status: str = "connected"
    workload: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CommandResult:
    """Result of a command execution."""
    success: bool
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    execution_time: float = 0.0
    error: Optional[str] = None
    data: Optional[Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ProcessInfo:
    """Information about a managed process."""
    process_id: str
    command: str
    status: ProcessStatus = ProcessStatus.RUNNING
    pid: Optional[int] = None
    start_time: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    end_time: Optional[str] = None
    exit_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    working_dir: str = "/kaggle/working"
    env: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['status'] = self.status.value
        return data


@dataclass
class FileInfo:
    """File/directory information."""
    name: str
    path: str
    is_dir: bool
    size: int = 0
    modified: str = ""
    permissions: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# Command payload builders
class CommandPayloads:
    """Helper class to build command payloads."""
    
    @staticmethod
    def file_upload(remote_path: str, content: str, encoding: str = "base64") -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_UPLOAD.value,
            "remote_path": remote_path,
            "content": content,
            "encoding": encoding
        }
    
    @staticmethod
    def file_download(remote_path: str) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_DOWNLOAD.value,
            "remote_path": remote_path
        }
    
    @staticmethod
    def file_delete(remote_path: str) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_DELETE.value,
            "remote_path": remote_path
        }
    
    @staticmethod
    def file_move(src: str, dst: str) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_MOVE.value,
            "src": src,
            "dst": dst
        }
    
    @staticmethod
    def file_copy(src: str, dst: str) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_COPY.value,
            "src": src,
            "dst": dst
        }
    
    @staticmethod
    def file_list(path: str = ".", recursive: bool = False) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_LIST.value,
            "path": path,
            "recursive": recursive
        }
    
    @staticmethod
    def file_mkdir(path: str, parents: bool = True, exist_ok: bool = True) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_MKDIR.value,
            "path": path,
            "parents": parents,
            "exist_ok": exist_ok
        }
    
    @staticmethod
    def file_rmdir(path: str, recursive: bool = False) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_RMDIR.value,
            "path": path,
            "recursive": recursive
        }
    
    @staticmethod
    def file_exists(path: str) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_EXISTS.value,
            "path": path
        }
    
    @staticmethod
    def file_stat(path: str) -> Dict[str, Any]:
        return {
            "command": CommandType.FILE_STAT.value,
            "path": path
        }
    
    @staticmethod
    def shell_exec(command: str, cwd: Optional[str] = None, 
                   env: Optional[Dict[str, str]] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
        return {
            "command": CommandType.SHELL_EXEC.value,
            "shell_command": command,
            "cwd": cwd,
            "env": env or {},
            "timeout": timeout
        }
    
    @staticmethod
    def shell_exec_async(command: str, cwd: Optional[str] = None,
                         env: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        return {
            "command": CommandType.SHELL_EXEC_ASYNC.value,
            "shell_command": command,
            "cwd": cwd,
            "env": env or {}
        }
    
    @staticmethod
    def process_start(command: str, cwd: Optional[str] = None,
                      env: Optional[Dict[str, str]] = None,
                      process_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "command": CommandType.PROCESS_START.value,
            "shell_command": command,
            "cwd": cwd,
            "env": env or {},
            "process_id": process_id or str(uuid.uuid4())
        }

    @staticmethod
    def process_restart(process_id: str) -> Dict[str, Any]:
        return {
            "command": CommandType.PROCESS_RESTART.value,
            "process_id": process_id
        }
    
    @staticmethod
    def process_terminate(process_id: str, signal: int = 15) -> Dict[str, Any]:
        return {
            "command": CommandType.PROCESS_TERMINATE.value,
            "process_id": process_id,
            "signal": signal
        }
    
    @staticmethod
    def process_list() -> Dict[str, Any]:
        return {
            "command": CommandType.PROCESS_LIST.value
        }
    
    @staticmethod
    def process_logs(process_id: str, tail: int = 100) -> Dict[str, Any]:
        return {
            "command": CommandType.PROCESS_LOGS.value,
            "process_id": process_id,
            "tail": tail
        }
    
    @staticmethod
    def process_wait(process_id: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        return {
            "command": CommandType.PROCESS_WAIT.value,
            "process_id": process_id,
            "timeout": timeout
        }
    
    @staticmethod
    def env_list() -> Dict[str, Any]:
        return {
            "command": CommandType.ENV_LIST.value
        }
    
    @staticmethod
    def env_get(key: str) -> Dict[str, Any]:
        return {
            "command": CommandType.ENV_GET.value,
            "key": key
        }
    
    @staticmethod
    def env_set(key: str, value: str) -> Dict[str, Any]:
        return {
            "command": CommandType.ENV_SET.value,
            "key": key,
            "value": value
        }
    
    @staticmethod
    def env_unset(key: str) -> Dict[str, Any]:
        return {
            "command": CommandType.ENV_UNSET.value,
            "key": key
        }
    
    @staticmethod
    def python_exec(code: str, capture_locals: bool = False) -> Dict[str, Any]:
        return {
            "command": CommandType.PYTHON_EXEC.value,
            "code": code,
            "capture_locals": capture_locals
        }
    
    @staticmethod
    def system_info() -> Dict[str, Any]:
        return {
            "command": CommandType.SYSTEM_INFO.value
        }
    
    @staticmethod
    def pwd() -> Dict[str, Any]:
        return {
            "command": CommandType.PWD.value
        }
    
    @staticmethod
    def cd(path: str) -> Dict[str, Any]:
        return {
            "command": CommandType.CD.value,
            "path": path
        }
    
    @staticmethod
    def notebook_ping() -> Dict[str, Any]:
        return {
            "command": CommandType.NOTEBOOK_PING.value
        }
    
    @staticmethod
    def notebook_restart() -> Dict[str, Any]:
        return {
            "command": CommandType.NOTEBOOK_RESTART.value
        }

    @staticmethod
    def notebook_shutdown(terminate_processes: bool = True) -> Dict[str, Any]:
        return {
            "command": CommandType.NOTEBOOK_SHUTDOWN.value,
            "terminate_processes": terminate_processes
        }

    # Proxy-level helpers (target = proxy, no notebook_id required)
    @staticmethod
    def proxy_list() -> Dict[str, Any]:
        return {"command": CommandType.PROXY_LIST.value}

    @staticmethod
    def proxy_info(notebook_id: str) -> Dict[str, Any]:
        return {"command": CommandType.PROXY_INFO.value, "notebook_id": notebook_id}

    @staticmethod
    def proxy_heartbeat() -> Dict[str, Any]:
        return {"command": CommandType.PROXY_HEARTBEAT.value}

    @staticmethod
    def proxy_stats() -> Dict[str, Any]:
        return {"command": CommandType.PROXY_STATS.value}


# Response payload builders
class ResponsePayloads:
    """Helper class to build response payloads."""
    
    @staticmethod
    def success(data: Any = None, **kwargs) -> Dict[str, Any]:
        result = {"success": True}
        if data is not None:
            result["data"] = data
        result.update(kwargs)
        return result
    
    @staticmethod
    def error(error: str, **kwargs) -> Dict[str, Any]:
        result = {"success": False, "error": error}
        result.update(kwargs)
        return result
    
    @staticmethod
    def command_result(result: CommandResult) -> Dict[str, Any]:
        return result.to_dict()
    
    @staticmethod
    def notebook_info(info: NotebookInfo) -> Dict[str, Any]:
        return info.to_dict()
    
    @staticmethod
    def notebook_list(notebooks: List[NotebookInfo]) -> Dict[str, Any]:
        return {"notebooks": [n.to_dict() for n in notebooks]}
    
    @staticmethod
    def process_info(info: ProcessInfo) -> Dict[str, Any]:
        return info.to_dict()
    
    @staticmethod
    def process_list(processes: List[ProcessInfo]) -> Dict[str, Any]:
        return {"processes": [p.to_dict() for p in processes]}
    
    @staticmethod
    def file_list(files: List[FileInfo]) -> Dict[str, Any]:
        return {"files": [f.to_dict() for f in files]}
    
    @staticmethod
    def file_info(info: FileInfo) -> Dict[str, Any]:
        return info.to_dict()
    
    @staticmethod
    def file_content(content: str, encoding: str = "base64") -> Dict[str, Any]:
        return {"content": content, "encoding": encoding}
    
    @staticmethod
    def env_list(env: Dict[str, str]) -> Dict[str, Any]:
        return {"environment": env}
    
    @staticmethod
    def env_value(key: str, value: Optional[str]) -> Dict[str, Any]:
        return {"key": key, "value": value}
    
    @staticmethod
    def python_result(result: Any, stdout: str = "", stderr: str = "",
                      exception: Optional[str] = None) -> Dict[str, Any]:
        return {
            "result": result,
            "stdout": stdout,
            "stderr": stderr,
            "exception": exception
        }
    
    @staticmethod
    def system_info(info: Dict[str, Any]) -> Dict[str, Any]:
        return info
    
    @staticmethod
    def pong() -> Dict[str, Any]:
        return {"status": "pong", "timestamp": datetime.utcnow().isoformat()}
    
    @staticmethod
    def pwd(path: str) -> Dict[str, Any]:
        return {"cwd": path}
    
    @staticmethod
    def cd(path: str) -> Dict[str, Any]:
        return {"cwd": path}