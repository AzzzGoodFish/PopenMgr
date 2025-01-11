import os
import subprocess
import time
import psutil
import tempfile
import threading
from io import BufferedReader
from logging import get_logger

from base.text import TextFileParser

LOGGER = get_logger(__name__)
STDOUT = 1
STDERR = 2

# 另一种防止 popen 阻塞的方法
# fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK)

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
LINE_BUFFER_CTL_LIB = os.path.join(FILE_DIR, "libstdbufctl.so")

class _StdStreamManager:
    def __init__(
        self,
        file_prefix: str,
        file_suffix: str,
        max_size: int = -1,
    ):
        self._max_size: int = max_size
        # 文件名、文本解析器、解析器 IO、popen 写 IO
        self._stream_collcet_file_path = tempfile.mktemp(prefix = file_prefix, suffix = file_suffix)
        self._stream_text_parser = TextFileParser(self._stream_collcet_file_path)
        self._stream_read_fileIO = self._stream_text_parser.fileIO
        self._stream_write_fileIO = open(self._stream_collcet_file_path, "wb")
        
        # 文件大小监控器
        self._stdout_semaphore: threading.Semaphore = None
        self._stderr_semaphore: threading.Semaphore = None
        self._size_monitor_signal = threading.Event()
        if self._max_size > 0:
            self._set_output_file_size_monitor()
            pass

    def __del__(self):
        self._size_monitor_signal.set()
        self._stream_text_parser.__del__()
        self._stream_write_fileIO.close()
    
    def delete_file(self):
        if os.path.exists(self._stream_collcet_file_path):
            os.remove(self._stream_collcet_file_path)
    
    def get_write_pipe(self):
        return self._stream_write_fileIO
    
    def get_read_pipe(self):
        return self._stream_read_fileIO
    
    def reset_write_pointer(self):
        self._stream_write_fileIO.seek(0)
        
    def reset_read_pointer(self):
        self._stream_read_fileIO.seek(0)
        
    def is_stream_end(self):
        return self._stream_text_parser.reach_end()

    def _set_output_file_size_monitor(self, monitor_interval: float = 0.3):
        def file_size_monitor(file_path: str, file_objs: list[BufferedReader], max_size: int, monitor_interval: float, exit_event: threading.Event):
            while not exit_event.is_set():
                file_size = os.path.getsize(file_path)
                beyond_size = file_size - max_size
                if beyond_size > max_size * 0.5:
                    LOGGER.warning(f"Output file {file_path} has a size of {file_size} bytes which is beyond the limit {max_size} bytes, clear file and reset IO pointer.")
                    for file_obj in file_objs:
                        file_obj.seek(0)
                        
                        if file_obj.writable():
                            file_obj.truncate(0)
                            
                        file_obj.flush()
                    
                time.sleep(monitor_interval)
    
        self._stdout_monitor_thread = threading.Thread(
            target = file_size_monitor,
            args = (self._stream_collcet_file_path, [self.get_read_pipe(), self.get_write_pipe()], self._max_size, monitor_interval, self._size_monitor_signal)
        )
        self._stdout_monitor_thread.start()
        
    def pick_lines(self, line_num: int, max_len_one_line: int = -1, strict_decode_mode: bool = False) -> tuple[list[str], int]:
        lines, size = self._stream_text_parser.readlines(line_num, max_len_one_line, strict_decode_mode)
        return lines, size    
    
    
class PopenProcMgr():
    class ProcResult:
        def __init__(self, returncode: int, stdout: list[str], stderr: list[str]):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr
    
    class TimeoutError(Exception):
        def __init__(self, cmd, timeout: int, stdout: list[str], stderr: list[str]):
            self.cmd = cmd
            self.timeout = timeout
            self.stdout = stdout
            self.stderr = stderr
            
        def __str__(self):
            return ("Command '%s' timed out after %s seconds" %(self.cmd, self.timeout))
    
    def __init__(
        self, 
        executable_path: str,
        args: list = [],
        cwd: str = ".",
        env: dict = None,
        collect_stdout: bool = True,
        collect_stderr: bool = True,
        merge_std_output: bool = False,
        set_line_buffered: bool = False,
        stdout_buffer_size: int = 1024 * 1024 * 100,
        stderr_buffer_size: int = 1024 * 1024 * 100,
        stdin_file: str = None,
        set_new_process_group: bool = False,
        label: str = "tmp",
    ):
        self._executable_path = executable_path
        self._args = args
        self._cwd = cwd
        self._env = env
        self._stdin_file_path = stdin_file
        self._stdin_file_stream = None
        
        if not os.path.exists(self._executable_path):
            self.__fix_executable_path()
        
        assert os.path.exists(self._executable_path) or os.system(f"which {self._executable_path} >/dev/null 2>&1") == 0, f"Executable path not exists: {self._executable_path}"
        assert os.path.exists(self._cwd), f"Working directory not exists: {self._cwd}"

        self._merget_option: bool = merge_std_output
        self._stdout_collect_option: bool = collect_stdout
        self._stderr_collect_option: bool = collect_stderr
        if self._merget_option:
            assert self._stdout_collect_option and self._stderr_collect_option, "Merge option must be used with stdout and stderr collecting on."
        
        if set_line_buffered:
            self.setup_line_buffered()
        
        self._stderr_manager: _StdStreamManager = None
        self._stdout_manager: _StdStreamManager = None
        self._proc_label = label
        self._stdout_manager_args = {
            "file_prefix": self._proc_label,
            "file_suffix": ".popen.stdout",
            "max_size": stdout_buffer_size
        }
        self._stderr_manager_args = {
            "file_prefix": self._proc_label,
            "file_suffix": ".popen.stderr",
            "max_size": stderr_buffer_size
        }
        self._setup_output_manager()
    
        # 生成 popen_args
        self._popen_args = { }
        if stdin_file:
            self._popen_args["bufsize"] = 0

        if set_new_process_group:
            self._popen_args["preexec_fn"] = os.setpgrp

        self._call_back: callable = None
        self._popen: subprocess.Popen = None
    
    def __fix_executable_path(self):        
        if run_bash_command(f"which {self._executable_path}").returncode == 0:
            return
        
        search_result = run_bash_command(
            command = f"which {self._executable_path}",
            env = self._env,
        )
        if search_result.returncode == 0 and search_result.stdout:
            self._executable_path = search_result.stdout[0].strip()
            return
      
    def setup_line_buffered(self):
        ld_preload_option = self._env.get("LD_PRELOAD", "")
        if ld_preload_option:
            ld_preload_option = f"{ld_preload_option}:" + LINE_BUFFER_CTL_LIB
        else:
            ld_preload_option = LINE_BUFFER_CTL_LIB
            
        self._env["LD_PRELOAD"] = ld_preload_option  
    
    def _setup_output_manager(self):
        if self._stdout_collect_option:
            self._stdout_manager = _StdStreamManager(**self._stdout_manager_args)
            
        if self._stderr_collect_option:
            if self._merget_option:
                self._stderr_manager = self._stdout_manager
            else:
                self._stderr_manager = _StdStreamManager(**self._stderr_manager_args)
        
    def clean(self, delete_output_file: bool = True):
        if not self._popen:
            return
        
        self.kill()
        if self._stdin_file_stream:
            self._stdin_file_stream.close()
            self._stdin_file_stream = None
        
        if self._stdout_manager:
            if delete_output_file:
                self._stdout_manager.delete_file()
            self._stdout_manager.__del__()
            self._stdout_manager = None
            
        if self._stderr_manager:
            if delete_output_file:
                self._stderr_manager.delete_file()
            self._stderr_manager.__del__()
            self._stderr_manager = None
        
        self._popen = None
        
    def __del__(self):
        self.clean()
        
    @property
    def running_time(self) -> float:
        return time.time() - self.start_time if self._popen else 0    
    
    @property
    def returncode(self) -> int | None:
        if self._popen == None:
            return None
        
        self._poll()
        return self._popen.returncode
    
    @property
    def cmd(self):
        return self._executable_path + " " + " ".join(self._args)

    @property
    def cmd_list(self):
        return [self._executable_path] + self._args

    @property
    def popen(self):
        return self._popen

    def set_proc_label(self, label: str):
        self._proc_label = label
        self._stdout_manager_args.update({"file_prefix": label})
        self._stderr_manager_args.update({"file_prefix": label})

    def append_args(self, args: list):
        self._args.extend(args)
    
    def is_living(self) -> bool:
        assert self._popen is not None, "Process is not started."
        return (self._popen.poll() is None)
    
    def _poll(self) -> int | None:
        assert self._popen is not None, "Process is not started."
        return self._popen.poll()
    
    def _kill_process_tree(self, pid: int) -> bool:
        try:
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            for child in children:
                try:
                    child.kill()
                    child.wait(1)
                except (psutil.AccessDenied, psutil.NoSuchProcess, OSError):
                    LOGGER.warning(f"Fail to kill process child: {child.pid}")
                    
            parent.kill()
            parent.wait(1)
            return not any(child.is_running() for child in children) and not parent.is_running()
        except (psutil.AccessDenied, psutil.NoSuchProcess, OSError):
            LOGGER.warning("Fail to find or access process.")
            return False
            
    def kill(self) -> bool:
        if self.is_living():
            return self._kill_process_tree(self._popen.pid)
            
    def start(self) -> None:
        assert self._popen is None, "Process is already started, call the clean() method to stop the previous one."
        
        self._setup_output_manager()
        if self._stdin_file_path:
            self._stdin_file_stream = open(self._stdin_file_path, 'r')
        
        if self._call_back:
            self._call_back(f"Start process: {self._executable_path} {' '.join(self._args)}")
            
        self.start_time = time.time()
        self._popen = subprocess.Popen(
            args = [self._executable_path] + self._args,
            cwd = self._cwd,
            env = self._env,
            stdin = self._stdin_file_stream,
            stdout = self._stdout_manager.get_write_pipe() if self._stdout_manager else subprocess.DEVNULL,
            stderr = self._stderr_manager.get_write_pipe() if self._stderr_manager else subprocess.DEVNULL,
            **self._popen_args
        )
        return
    
    def _wait(self, timeout: float = None, del_tmpfile: bool = True) -> ProcResult:
        end_time = None
        if timeout:
            end_time = time.monotonic() + timeout

        stdout_lines = []
        stderr_lines = []
        while self.still_remain_output_to_read() or self.is_living():
            stderr_buffer, read_len = self.pick_stderr()
            for line in stderr_buffer:
                if self._call_back:
                    self._call_back(f"[proc: {self._proc_label}] [stderr] {line}")
                stderr_lines.append(line)
                
            stdout_buffer, read_len = self.pick_stdout()
            for line in stdout_buffer:
                if self._call_back:
                    self._call_back(f"[proc: {self._proc_label}] [stdout] {line}")
                stdout_lines.append(line)
                
            if timeout:
                remaining_time = end_time - time.monotonic()
                if remaining_time <= 0:
                    self.kill()
                    raise self.TimeoutError(self.cmd, timeout, stdout_lines, stderr_lines)

        result = self.ProcResult(self.returncode, stdout_lines, stderr_lines)
        self.clean(del_tmpfile)
        return result
    
    def run(self, timeout: float = None, del_tmpfile: bool = True) -> ProcResult:
        self.start()
        return self._wait(timeout, del_tmpfile)
    
    def wait(self, timeout: float = None) -> ProcResult:
        return self._wait(timeout)
    
    def pick_stdout(self, line_num: int = -1, max_len_one_line: int = -1, strict_decode_mode: bool = False) -> tuple[list[str], int]:
        if self._stdout_manager == None:
            return [], 0
        
        return self._stdout_manager.pick_lines(line_num, max_len_one_line, strict_decode_mode)
    
    def pick_stderr(self, line_num: int = -1, max_len_one_line: int = -1, strict_decode_mode: bool = False) -> tuple[list[str], int]:
        if self._merget_option:
            return [], 0
                
        if self._stderr_manager == None:
            return [], 0
        
        return self._stderr_manager.pick_lines(line_num, max_len_one_line, strict_decode_mode)
    
    def set_log_callback(self, log_callback: callable):
        self._call_back = log_callback
    
    def still_remain_output_to_read(self) -> bool:
        has_stdout = self._stdout_manager.is_stream_end() == False if self._stdout_manager else False
        has_stderr = self._stderr_manager.is_stream_end() == False if self._stderr_manager else False
        return has_stdout or has_stderr


def run_bash_command(command: str, cwd: str = ".", env: dict = None, timeout: float = None, merge_stderr: bool = False) -> PopenProcMgr.ProcResult:
    proc = PopenProcMgr(
        executable_path = "/bin/bash",
        args = ["-c", command],
        cwd = cwd,
        env = env,
        merge_std_output = merge_stderr,
    )
    return proc.run(timeout)


def run_bash_script(script_path: str, cwd: str = ".", env: dict = None, timeout: float = None, merge_stderr: bool = False) -> PopenProcMgr.ProcResult:
    proc = PopenProcMgr(
        executable_path = "/bin/bash",
        args = [script_path],
        cwd = cwd,
        env = env,
        merge_std_output = merge_stderr,
    )
    return proc.run(timeout)
