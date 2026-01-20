import csv
import json
import mimetypes
import os
import pathlib
import subprocess
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BASE_DIR = pathlib.Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"
SSH_CONFIG_PATH = os.environ.get(
    "SSH_CONFIG_PATH", os.path.expanduser("~/.ssh/config")
)
SSH_CONTROL_PATH = os.environ.get("SSH_CONTROL_PATH")
if SSH_CONTROL_PATH:
    SSH_CONTROL_PATH = os.path.expanduser(SSH_CONTROL_PATH)
SSH_CONTROL_PERSIST = os.environ.get("SSH_CONTROL_PERSIST", "60s")
SSH_USE_CONTROL = bool(SSH_CONTROL_PATH) and os.name != "nt"
SSH_CONNECT_TIMEOUT = int(os.environ.get("SSH_CONNECT_TIMEOUT", "15"))
SSH_FILE_TIMEOUT = int(os.environ.get("SSH_FILE_TIMEOUT", "45"))
GPU_QUERY = (
    "nvidia-smi --query-gpu=index,name,temperature.gpu,"
    "utilization.gpu,memory.used,memory.total "
    "--format=csv,noheader,nounits"
)
GPU_PROCESS_SCRIPT = r"""
import csv
import json
import os
import subprocess
import sys


def run(cmd):
    process = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if process.returncode != 0:
        sys.stderr.write(process.stderr or process.stdout)
        sys.exit(process.returncode)
    return process.stdout.strip()


def parse_gpu_map(text):
    mapping = {}
    for row in csv.reader(text.splitlines()):
        if len(row) < 2:
            continue
        index, uuid = [item.strip() for item in row[:2]]
        mapping[uuid] = index
    return mapping


def parse_processes(text, mapping):
    processes = []
    if not text:
        return processes
    for row in csv.reader(text.splitlines()):
        if len(row) < 4:
            continue
        uuid, pid, name, mem = [item.strip() for item in row[:4]]
        gpu_index = mapping.get(uuid, "")
        cwd = ""
        cwd_error = ""
        if pid.isdigit():
            try:
                cwd = os.readlink("/proc/{}/cwd".format(pid))
            except Exception as exc:
                cwd_error = str(exc)
        mem_used = None
        if mem:
            try:
                mem_used = int(float(mem))
            except ValueError:
                mem_used = None
        processes.append(
            {
                "gpu_index": int(gpu_index) if gpu_index.isdigit() else None,
                "pid": int(pid) if pid.isdigit() else None,
                "name": name,
                "mem_used": mem_used,
                "cwd": cwd,
                "cwd_error": cwd_error,
            }
        )
    return processes


try:
    gpu_text = run(
        "nvidia-smi --query-gpu=index,uuid --format=csv,noheader,nounits"
    )
    proc_text = run(
        "nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_memory "
        "--format=csv,noheader,nounits"
    )
except Exception as exc:
    sys.stderr.write(str(exc))
    sys.exit(1)

if proc_text.strip().lower().startswith("no running processes"):
    proc_text = ""

mapping = parse_gpu_map(gpu_text)
processes = parse_processes(proc_text, mapping)
print(json.dumps(processes))
"""


def parse_ssh_config(path_str):
    path = pathlib.Path(path_str)
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return []

    hosts = []
    seen = set()
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if not parts:
            continue
        if parts[0].lower() == "host":
            for host in parts[1:]:
                if any(ch in host for ch in "*?!"):
                    continue
                if host not in seen:
                    seen.add(host)
                    hosts.append(host)
    return hosts


def parse_ssh_config_users(path_str):
    path = pathlib.Path(path_str)
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except FileNotFoundError:
        return {}, ""

    user_map = {}
    default_user = ""
    current_hosts = []
    current_has_wildcard = False
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if not parts:
            continue
        keyword = parts[0].lower()
        if keyword == "host":
            current_hosts = []
            current_has_wildcard = False
            for host in parts[1:]:
                if any(ch in host for ch in "*?!"):
                    current_has_wildcard = True
                    continue
                current_hosts.append(host)
            continue
        if keyword == "user" and len(parts) >= 2:
            user_value = parts[1]
            if current_hosts:
                for host in current_hosts:
                    user_map.setdefault(host, user_value)
            elif current_has_wildcard and not default_user:
                default_user = user_value

    return user_map, default_user


SSH_USER_MAP, SSH_DEFAULT_USER = parse_ssh_config_users(SSH_CONFIG_PATH)


def _ssh_user_for_host(host):
    if not host:
        return ""
    return SSH_USER_MAP.get(host) or SSH_DEFAULT_USER


def _ssh_base_cmd(host=None):
    cmd = [
        "ssh",
        "-F",
        SSH_CONFIG_PATH,
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",
        "-o",
        "ClearAllForwardings=yes",
    ]
    user = _ssh_user_for_host(host)
    if user:
        cmd.extend(["-o", f"User={user}"])
    if SSH_USE_CONTROL:
        cmd.extend(
            [
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPersist={SSH_CONTROL_PERSIST}",
                "-o",
                f"ControlPath={SSH_CONTROL_PATH}",
            ]
        )
    return cmd


def _sftp_base_cmd(host=None):
    cmd = [
        "sftp",
        "-F",
        SSH_CONFIG_PATH,
        "-o",
        f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",
        "-o",
        "BatchMode=yes",
        "-o",
        "ClearAllForwardings=yes",
    ]
    user = _ssh_user_for_host(host)
    if user:
        cmd.extend(["-o", f"User={user}"])
    return cmd


def _quote_sh(value):
    if value is None:
        return "''"
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _quote_sftp_path(value):
    if value is None:
        return '""'
    return '"' + value.replace('"', '\\"') + '"'


def _sftp_local_path(path):
    return path.replace("\\", "/")


def _ssh_error_text(result):
    return (result.stderr or result.stdout or "").strip()


def _run_ssh(host):
    cmd = _ssh_base_cmd(host)
    cmd.extend([host, GPU_QUERY])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"host": host, "ok": False, "error": "ssh timed out", "gpus": []}

    if result.returncode != 0:
        error_text = _ssh_error_text(result)
        if not error_text:
            error_text = f"ssh exited with {result.returncode}"
        return {"host": host, "ok": False, "error": error_text, "gpus": []}

    output = result.stdout.strip()
    if not output:
        return {"host": host, "ok": False, "error": "no data from nvidia-smi", "gpus": []}

    gpus = []
    reader = csv.reader(output.splitlines())
    for row in reader:
        row = [item.strip() for item in row]
        if len(row) < 6:
            continue
        try:
            index = int(row[0])
            name = row[1]
            temp = int(float(row[2]))
            util = int(float(row[3]))
            mem_used = int(float(row[4]))
            mem_total = int(float(row[5]))
        except ValueError:
            continue
        gpus.append(
            {
                "index": index,
                "name": name,
                "temp": temp,
                "util": util,
                "mem_used": mem_used,
                "mem_total": mem_total,
            }
        )

    if not gpus:
        return {
            "host": host,
            "ok": False,
            "error": "unable to parse nvidia-smi output",
            "gpus": [],
        }

    util_avg = round(sum(gpu["util"] for gpu in gpus) / len(gpus))
    mem_used_total = sum(gpu["mem_used"] for gpu in gpus)
    mem_total_total = sum(gpu["mem_total"] for gpu in gpus)
    mem_pct = round((mem_used_total / mem_total_total) * 100) if mem_total_total else 0

    return {
        "host": host,
        "ok": True,
        "summary": {
            "count": len(gpus),
            "util_avg": util_avg,
            "mem_used": mem_used_total,
            "mem_total": mem_total_total,
            "mem_pct": mem_pct,
        },
        "gpus": gpus,
    }


def _run_ssh_processes(host):
    cmd = _ssh_base_cmd(host)
    cmd.extend(
        [
            host,
            "sh",
            "-c",
            "command -v python3 >/dev/null 2>&1 && exec python3 - || exec python -",
        ]
    )
    try:
        result = subprocess.run(
            cmd,
            input=GPU_PROCESS_SCRIPT,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"host": host, "ok": False, "error": "ssh timed out", "processes": []}

    if result.returncode != 0:
        error_text = _ssh_error_text(result)
        if not error_text:
            error_text = f"ssh exited with {result.returncode}"
        return {"host": host, "ok": False, "error": error_text, "processes": []}

    output = result.stdout.strip()
    if not output:
        return {"host": host, "ok": True, "processes": []}

    try:
        processes = json.loads(output)
    except json.JSONDecodeError:
        return {
            "host": host,
            "ok": False,
            "error": "invalid process data",
            "processes": [],
        }

    return {"host": host, "ok": True, "processes": processes}


def _remote_file_size(host, remote_path):
    cmd = _ssh_base_cmd(host)
    quoted = _quote_sh(remote_path)
    cmd.extend([host, "sh", "-c", f"ls -ln -- {quoted}"])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SSH_FILE_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, "ssh timed out"

    if result.returncode != 0:
        error_text = _ssh_error_text(result) or f"ssh exited with {result.returncode}"
        return None, error_text

    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        return None, "invalid file size"
    parts = lines[-1].split()
    if len(parts) < 5:
        return None, "invalid file size"
    size_text = parts[4]
    try:
        return int(size_text), ""
    except ValueError:
        return None, "invalid file size"


def _upload_via_ssh(host, remote_path, source, length):
    cmd = _ssh_base_cmd(host)
    quoted = _quote_sh(remote_path)
    cmd.extend([host, "sh", "-c", f"cat > {quoted}"])
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    remaining = length
    error_text = ""
    try:
        while remaining > 0:
            chunk = source.read(min(65536, remaining))
            if not chunk:
                break
            proc.stdin.write(chunk)
            remaining -= len(chunk)
    except BrokenPipeError:
        error_text = "ssh failed during upload"
    except ConnectionResetError:
        error_text = "client disconnected"
    finally:
        if proc.stdin:
            try:
                proc.stdin.close()
            except Exception:
                pass

    try:
        stdout, stderr = proc.communicate(timeout=300)
    except subprocess.TimeoutExpired:
        proc.kill()
        return {"ok": False, "error": "upload timed out"}

    if remaining > 0 and not error_text:
        error_text = "upload interrupted"
    if proc.returncode != 0 and not error_text:
        error_text = (stderr or stdout or b"").decode("utf-8", errors="ignore").strip()
        if not error_text:
            error_text = f"ssh exited with {proc.returncode}"

    if error_text:
        return {"ok": False, "error": error_text}
    return {"ok": True}


def _download_via_sftp(host, remote_path):
    temp_dir = tempfile.mkdtemp(prefix="gpu_monitor_")
    tmp_path = os.path.join(temp_dir, f"download_{uuid.uuid4().hex}")
    local_path = _sftp_local_path(tmp_path)
    cmd = _sftp_base_cmd(host)
    cmd.extend(["-b", "-", host])
    script = f"get {_quote_sftp_path(remote_path)} {_quote_sftp_path(local_path)}\n"
    try:
        result = subprocess.run(
            cmd,
            input=script.encode("utf-8"),
            capture_output=True,
            timeout=SSH_FILE_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        try:
            os.rmdir(temp_dir)
        except OSError:
            pass
        return None, None, "ssh timed out"
    if result.returncode != 0:
        error_text = _ssh_error_text(result)
        if not error_text:
            error_text = f"sftp exited with {result.returncode}"
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        try:
            os.rmdir(temp_dir)
        except OSError:
            pass
        return None, None, error_text
    return tmp_path, temp_dir, ""

def fetch_gpu_processes(host, index):
    result = _run_ssh_processes(host)
    if not result.get("ok"):
        result["index"] = index
        return result
    processes = result.get("processes", [])
    filtered = [item for item in processes if item.get("gpu_index") == index]
    return {"host": host, "ok": True, "index": index, "processes": filtered}


def fetch_statuses(hosts):
    if not hosts:
        return []

    max_workers = min(8, len(hosts))
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_run_ssh, host): host for host in hosts}
        for future in as_completed(future_map):
            host = future_map[future]
            try:
                results[host] = future.result()
            except Exception as exc:
                results[host] = {
                    "host": host,
                    "ok": False,
                    "error": f"error: {exc}",
                    "gpus": [],
                }

    ordered = []
    for host in hosts:
        ordered.append(results.get(host, {"host": host, "ok": False, "error": "missing"}))
    return ordered


class GPURequestHandler(BaseHTTPRequestHandler):
    def _safe_write(self, data):
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return False
        return True

    def _send_json(self, payload, status=HTTPStatus.OK):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self._safe_write(data)

    def _send_text(self, message, status=HTTPStatus.BAD_REQUEST):
        data = message.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self._safe_write(data)

    def _serve_static(self, rel_path):
        if rel_path == "/":
            rel_path = "/index.html"
        candidate = (WEB_DIR / rel_path.lstrip("/")).resolve()
        try:
            candidate.relative_to(WEB_DIR)
        except ValueError:
            self._send_text("invalid path", status=HTTPStatus.NOT_FOUND)
            return

        if not candidate.is_file():
            self._send_text("not found", status=HTTPStatus.NOT_FOUND)
            return

        mime_type, _ = mimetypes.guess_type(str(candidate))
        if not mime_type:
            mime_type = "application/octet-stream"
        data = candidate.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self._safe_write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/servers":
            hosts = parse_ssh_config(SSH_CONFIG_PATH)
            self._send_json({"hosts": hosts, "config": SSH_CONFIG_PATH})
            return
        if parsed.path == "/api/status":
            query = parse_qs(parsed.query)
            host = (query.get("host") or [None])[0]
            if not host:
                self._send_text("missing host", status=HTTPStatus.BAD_REQUEST)
                return
            status = fetch_statuses([host])[0]
            self._send_json(status)
            return
        if parsed.path == "/api/gpu-processes":
            query = parse_qs(parsed.query)
            host = (query.get("host") or [None])[0]
            index_raw = (query.get("index") or [None])[0]
            if not host or index_raw is None:
                self._send_json(
                    {"ok": False, "error": "missing host or index"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            try:
                index = int(index_raw)
            except ValueError:
                self._send_json(
                    {"ok": False, "error": "invalid index"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            result = fetch_gpu_processes(host, index)
            self._send_json(result)
            return
        if parsed.path == "/api/download":
            query = parse_qs(parsed.query)
            host = (query.get("host") or [None])[0]
            remote_path = (query.get("path") or [None])[0]
            if not host or not remote_path:
                self._send_json(
                    {"ok": False, "error": "missing host or path"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            filename = os.path.basename(remote_path) or "download.bin"
            safe_name = filename.replace('"', "_")
            tmp_path, temp_dir, error_text = _download_via_sftp(host, remote_path)
            if error_text:
                self._send_json(
                    {"ok": False, "error": error_text},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            try:
                file_size = os.path.getsize(tmp_path)
            except OSError:
                file_size = None
            try:
                stream = open(tmp_path, "rb")
            except OSError as exc:
                self._send_json(
                    {"ok": False, "error": str(exc)},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                if temp_dir:
                    try:
                        os.rmdir(temp_dir)
                    except OSError:
                        pass
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/octet-stream")
            if file_size is not None:
                self.send_header("Content-Length", str(file_size))
            self.send_header("Content-Disposition", f'attachment; filename="{safe_name}"')
            self.end_headers()
            try:
                while True:
                    chunk = stream.read(65536)
                    if not chunk:
                        break
                    if not self._safe_write(chunk):
                        break
            finally:
                try:
                    stream.close()
                except Exception:
                    pass
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                if temp_dir:
                    try:
                        os.rmdir(temp_dir)
                    except OSError:
                        pass
            return
        self._serve_static(parsed.path)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length).decode("utf-8") if length else ""
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                self._send_text("invalid json", status=HTTPStatus.BAD_REQUEST)
                return

            hosts = payload.get("hosts")
            if not isinstance(hosts, list) or not all(isinstance(h, str) for h in hosts):
                hosts = parse_ssh_config(SSH_CONFIG_PATH)

            results = fetch_statuses(hosts)
            self._send_json({"results": results})
            return

        if parsed.path == "/api/upload":
            query = parse_qs(parsed.query)
            host = (query.get("host") or [None])[0]
            remote_path = (query.get("path") or [None])[0]
            filename = (query.get("name") or [None])[0]
            if not host or not remote_path:
                self._send_json(
                    {"ok": False, "error": "missing host or path"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            if remote_path.endswith("/"):
                if not filename:
                    self._send_json(
                        {"ok": False, "error": "missing filename"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                remote_path = remote_path + filename

            length_header = self.headers.get("Content-Length")
            if length_header is None:
                self._send_json(
                    {"ok": False, "error": "missing content length"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            try:
                length = int(length_header or 0)
            except ValueError:
                self._send_json(
                    {"ok": False, "error": "invalid content length"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            result = _upload_via_ssh(host, remote_path, self.rfile, length)
            status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
            self._send_json(result, status=status)
            return

        self._send_text("not found", status=HTTPStatus.NOT_FOUND)


def main():
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), GPURequestHandler)
    print(f"GPU Monitor running on http://localhost:{port}")
    print(f"Using SSH config: {SSH_CONFIG_PATH}")
    server.serve_forever()


if __name__ == "__main__":
    main()
