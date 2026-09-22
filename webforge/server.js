const http = require('http');
const fs = require('fs');
const path = require('path');
const publicDir = path.join(__dirname, 'public');
const port = process.env.PORT || 3000;
const types = {'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml','.png':'image/png','.ico':'image/x-icon'};
http.createServer((req,res)=>{
  const raw = decodeURIComponent((req.url || '/').split('?')[0]);
  const clean = raw === '/' ? '/index.html' : raw;
  const file = path.normalize(path.join(publicDir, clean));
  if (!file.startsWith(publicDir)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(file,(err,data)=>{
    if (err) {
      fs.readFile(path.join(publicDir,'index.html'),(e,fallback)=>{
        if(e){res.writeHead(404);return res.end('Not found');}
        res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});
        res.end(fallback);
      });
      return;
    }
    res.writeHead(200,{'Content-Type':types[path.extname(file)] || 'application/octet-stream','Cache-Control':path.extname(file)==='.html'?'no-cache':'public, max-age=3600'});
    res.end(data);
  });
}).listen(port,'0.0.0.0',()=>console.log('WebForge AI listening on',port));