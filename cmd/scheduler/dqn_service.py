import json
import os
import random
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

import torch
import torch.nn as nn


class DQN(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(18, 128),
            nn.ReLU(),
            nn.Linear(128, 128),
            nn.ReLU(),
            nn.Linear(128, 3)
        )

    def forward(self, x):
        return self.net(x)


# Load the trained model
model = DQN()
model_path = os.path.join(os.path.dirname(__file__), "dqn_model.pth")

try:
    model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu'), weights_only=True))
    model.eval()
    print(f"Loaded DQN model from {model_path}")
except FileNotFoundError:
    print(f"WARNING: Model file {model_path} not found, using random actions")
    # Initialize with random weights
    pass


class DQNHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/predict':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))

            state = data['state']
            state_tensor = torch.FloatTensor(state).unsqueeze(0)

            with torch.no_grad():
                q_values = model(state_tensor)
                
                # FIXED: Epsilon-greedy exploration
                epsilon = 0.30  # 30% random exploration
                if random.random() < epsilon:
                    action = random.randint(0, 2)
                    print(f"EXPLORATION: random action={action}, epsilon={epsilon}")
                else:
                    action = torch.argmax(q_values).item()
                    print(f"EXPLOITATION: action={action}, q_values={q_values.squeeze().tolist()}")

            response = {
                "action": action,
                "q_values": q_values.squeeze().tolist()
            }

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def run(port=5010):
    server_address = ('', port)
    httpd = ThreadedHTTPServer(server_address, DQNHandler)
    print(f"DQN sidecar service running on port {port}...")
    httpd.serve_forever()


if __name__ == '__main__':
    run()