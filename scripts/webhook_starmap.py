#!/usr/bin/env python3
"""
桌游星图 — GitHub Webhook 自动拉取
"""

import http.server
import json
import hmac
import hashlib
import subprocess
import logging
import threading
from datetime import datetime

PORT = 9000
REPO_DIR = "/root/boardgame-starmap"
LOG_FILE = "/var/log/starmap_webhook.log"
WEBHOOK_SECRET = "starmap_webhook_2026"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("webhook")

# 同一时刻只允许一个 git pull 执行，避免并发推送时互相打架
pull_lock = threading.Lock()


def do_git_pull():
    with pull_lock:
        try:
            log.info("🔄 开始 git pull...")
            result = subprocess.run(
                ["bash", "-c", f"cd {REPO_DIR} && git checkout -f && git pull origin main"],
                capture_output=True, text=True, timeout=60
            )
            if result.returncode == 0:
                log.info(f"✅ git pull 成功: {result.stdout.strip()}")
                return True
            else:
                log.error(f"❌ git pull 失败: {result.stderr.strip()}")
                return False
        except subprocess.TimeoutExpired:
            log.error("❌ git pull 超时（60s）")
            return False
        except Exception as e:
            log.error(f"❌ git pull 异常: {e}")
            return False


def verify_signature(payload_body, signature_header):
    if not signature_header:
        return False
    expected = hmac.new(
        WEBHOOK_SECRET.encode("utf-8"),
        payload_body,
        hashlib.sha256
    ).hexdigest()
    received = signature_header.replace("sha256=", "")
    return hmac.compare_digest(expected, received)


class WebhookHandler(http.server.BaseHTTPRequestHandler):
    # 防止慢速 / 半开连接永久占用一个线程
    timeout = 30

    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return
        content_length = int(self.headers.get("Content-Length", 0))
        payload_body = self.rfile.read(content_length)
        signature = self.headers.get("X-Hub-Signature-256", "")
        if not verify_signature(payload_body, signature):
            log.warning(f"⚠️ 签名验证失败，来源 IP: {self.client_address[0]}")
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Invalid signature")
            return
        event = self.headers.get("X-GitHub-Event", "unknown")
        log.info(f"📨 收到 GitHub 事件: {event}")
        if event == "push":
            try:
                payload = json.loads(payload_body)
                ref = payload.get("ref", "")
                pusher = payload.get("pusher", {}).get("name", "unknown")
                commits = payload.get("commits", [])
                log.info(f"   分支: {ref}, 推送者: {pusher}, 提交数: {len(commits)}")
            except Exception:
                pass
            success = do_git_pull()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK" if success else b"Pull failed")
        elif event == "ping":
            log.info("   🏓 Ping 事件（webhook 配置验证）")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"pong")
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Ignored")

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(f"OK - {datetime.now().isoformat()}".encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


class ThreadedServer(http.server.ThreadingHTTPServer):
    # 线程随进程退出，避免僵死线程阻止关停
    daemon_threads = True
    # 扩大 accept 队列，容忍瞬时并发
    request_queue_size = 64
    allow_reuse_address = True


if __name__ == "__main__":
    log.info(f"🚀 Webhook 服务启动 — 端口 {PORT}（多线程模式）")
    server = ThreadedServer(("0.0.0.0", PORT), WebhookHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
