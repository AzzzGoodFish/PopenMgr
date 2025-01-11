import os
import chardet
from functools import cache
from io import BufferedReader
from termcolor import colored
from logging import get_logger

from base.error import CustomError
from base.decorator import time_costing

logger_text_parser = get_logger(__name__)

class TextFileParser:
    def __init__(self, file_path: str, default_encoding: str = "utf-8"):
        self._file_path: str = file_path
        self._default_encoding: str = default_encoding
        self._fixed_encoding: str = ""
        
        if not os.path.exists(self._file_path):
            # 创建文件
            with open(self._file_path, "w"):
                pass
            
        self._fileIO = open(self._file_path, "rb+")
            
    def __del__(self):
        if self._fileIO:
            self._fileIO.close()
        
    @property
    def file_path(self):
        return self._file_path
    
    @property
    def fileIO(self):
        return self._fileIO
    
    @property
    def default_encoding(self):
        return self._default_encoding
    
    @property
    def fixed_encoding(self):
        return self._fixed_encoding
    
    def reach_end(self) -> bool:
        return self.fileIO.tell() >= os.path.getsize(self.file_path)
    
    def move_cursor(self, offset: int) -> int:
        if offset == 0:
            return self.fileIO.tell()
        
        return self.fileIO.seek(self._fileIO.tell() + offset)
    
    def readlines(self, line_num: int = 1, max_len_one_line: int = -1, strict_decode = False) -> tuple[list[str], int]:
        return self.__readlines_with_encoding_fix(line_num, max_len_one_line, strict_decode)
    
    # def pop_lines(self, line_num: int = 1, max_len_one_line: int = -1, strict_decode = False) -> list[str] | None:
    #     return self.__readlines_with_encoding_fix(line_num, max_len_one_line, strict_decode, delete_after_read = False)
                
    # @time_costing
    def __readlines_with_encoding_fix(self, line_num_to_read: int = -1, max_len_one_line: int = -1, strict_decode = False) -> tuple[list[str], int]:
        errors_strategy = 'strict'
        encoding = self._default_encoding if (self._fixed_encoding == "") else self._fixed_encoding
        
        read_size = 0
        lines = []
        raw_line = b''
        
        while (line_num_to_read == -1) or (len(lines) < line_num_to_read):
            raw_line = self._fileIO.readline()
            read_size += len(raw_line)
            raw_line = raw_line[:max_len_one_line]
            if len(raw_line) == 0:
                break
            
            for retry_count in range(3):
                try:
                    decoded_line = raw_line.decode(encoding, errors = errors_strategy)
                    lines.append(decoded_line)
                    break
                except UnicodeDecodeError:
                    fixed_encoding  = detect_bytes_encoding(raw_line)
                    if strict_decode and fixed_encoding is None:
                        raise CustomError(f"Read file in strict mode but failed to decode line even after detecting its encoding, line in bytes: {raw_line}")
                    
                    # 无法检测到编码时, 使用默认编码并忽略解码错误
                    if fixed_encoding is None:
                        errors_strategy = 'replace'
                        continue
                    
                    encoding = fixed_encoding
                    self._fixed_encoding = encoding
                    continue
                
        return lines, read_size    
          
          
@cache
def detect_file_encoding(file_path: str) -> str | None:
    if not os.path.exists(file_path):
        logger_text_parser.error(
            f"The file being detected does not exist: {file_path}")
        return None

    try:
        with open(file_path, "rb") as file:
            text = file.read()
            return detect_bytes_encoding(text)
    except Exception as e:
        logger_text_parser.error(f"Failed to detect encoding: {e}")
        return None


def detect_bytes_encoding(text: bytes) -> str | None:
    # 检查参数类型
    if not isinstance(text, bytes):
        logger_text_parser.error("TypeError: text is not bytes")
        return None

    # 使用 chardet 检测编码
    encoding = chardet.detect(text)
    logger_text_parser.debug(f"Detected encoding: {encoding}")

    # 将 ascii 和 utf-8 视为同一种编码
    if encoding["encoding"] in ("utf-8", "ascii"):
        return 'utf-8'
    else:
        return encoding["encoding"]


def trans_bytes_2_utf8(text: bytes) -> str | None:
    encoding = detect_bytes_encoding(text)
    if encoding is None:
        logger_text_parser.error(
            "trans_str_utf8 failed: detect_encoding failed")
        return None

    try:
        return text.decode(encoding)
    except UnicodeDecodeError:
        logger_text_parser.warning(
            f"using {encoding} to decode failed, set errors = 'replace'")
        return text.decode(encoding, errors="replace")


def trans_file_2_utf8(file_in_path: str, file_out_path: str) -> bool:
    if not os.path.exists(file_in_path):
        logger_text_parser.error(
            f"the file being converted does not exist: {file_in_path}")
        return False

    if os.path.exists(file_out_path):
        logger_text_parser.error(
            f"the file to be converted already exists: {file_out_path}")
        return False

    try:
        with open(file_in_path, "rb") as file_in:
            text = file_in.read()
            text_utf8 = trans_bytes_2_utf8(text)

        if text_utf8 is None:
            logger_text_parser.error("")

        with open(file_out_path, "w", encoding="utf-8") as file_out:
            file_out.write(text_utf8)

        return True

    except Exception as e:
        logger_text_parser.error(f"Failed to convert file: {e}")
        return False


def read_text_range(file_path: str, start_line: int, end_line: int) -> tuple[str, int, int]:
    file_encoding = detect_file_encoding(file_path)
    errors_strategy = None
    if file_encoding is None:
        errors_strategy = "replace"
    
    with open(file_path, "r", encoding = file_encoding, errors = errors_strategy) as file:
        lines = file.readlines()
        if len(lines) < end_line:
            end_line = len(lines)
            
        return "".join(lines[start_line - 1: end_line]), start_line, end_line
