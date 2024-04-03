from http.server import BaseHTTPRequestHandler, HTTPServer
from apis import MyHandler

server_address = ('', 8000)
httpd = HTTPServer(server_address, MyHandler)
httpd.serve_forever()