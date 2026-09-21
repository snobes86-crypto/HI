import http from "node:http";
import crypto from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const TTL_SESSION = 60 * 60 * 24 * 7;
const TTL_PAIR = 60 * 30;
const memory = new Map();
function now() { return Date.now(); }
class MemoryStore {
  async set(key, value, opts={}) { memory.set(key,{value,expires:opts.ex?now()+opts.ex*1000:null}); return "OK"; }
  async get(key) { const item=memory.get(key); if(!item) return null; if(item.expires && item.expires<now()){memory.delete(key);return null;} return item.value; }
  async del(key){ memory.delete(key); return 1; }
  async rpush(key,value){ const current=(await this.get(key))||[]; current.push(value); await this.set(key,current,{ex:TTL_SESSION}); return current.length; }
  async lpop(key){ const current=(await this.get(key))||[]; const value=current.shift()??null; await this.set(key,current,{ex:TTL_SESSION}); return value; }
  async expire(key,seconds){ const item=memory.get(key); if(item) item.expires=now()+seconds*1000; return item?1:0; }
}
const memoryStore = new MemoryStore();
function store(){ return memoryStore; }

const redis = () => store();
const key = (kind, id) => `rsgb:${kind}:${id}`;

function randomId(prefix = "") {
  return prefix + crypto.randomBytes(18).toString("base64url");
}
function pairingCode() {
  return String(crypto.randomInt(100000, 1000000));
}
async function createSession(projectName = "Roblox Game") {
  const sessionId = randomId("ses_");
  const code = pairingCode();
  const session = {
    sessionId, projectName,
    createdAt: new Date().toISOString(),
    connected: false, lastSeenAt: null, studioToken: null,
    lastLogs: [], scripts: {}, instances: {}
  };
  await redis().set(key("session", sessionId), session, { ex: TTL_SESSION });
  await redis().set(key("pair", code), { sessionId }, { ex: TTL_PAIR });
  return { sessionId, code };
}
async function getSession(sessionId) {
  return await redis().get(key("session", sessionId));
}
async function saveSession(session) {
  await redis().set(key("session", session.sessionId), session, { ex: TTL_SESSION });
}
async function pairStudio(code, studioInfo = {}) {
  const pair = await redis().get(key("pair", String(code)));
  if (!pair?.sessionId) return null;
  const session = await getSession(pair.sessionId);
  if (!session) return null;
  const studioToken = randomId("stu_");
  session.connected = true;
  session.lastSeenAt = new Date().toISOString();
  session.studioToken = studioToken;
  session.studioInfo = studioInfo;
  await saveSession(session);
  await redis().set(key("studio", studioToken), { sessionId: session.sessionId }, { ex: TTL_SESSION });
  await redis().del(key("pair", String(code)));
  return { sessionId: session.sessionId, studioToken, projectName: session.projectName };
}
async function getSessionByStudioToken(studioToken) {
  const map = await redis().get(key("studio", studioToken));
  if (!map?.sessionId) return null;
  return await getSession(map.sessionId);
}
async function touchStudio(studioToken, studioInfo = null) {
  const session = await getSessionByStudioToken(studioToken);
  if (!session) return null;
  session.connected = true;
  session.lastSeenAt = new Date().toISOString();
  if (studioInfo) session.studioInfo = studioInfo;
  await saveSession(session);
  await redis().expire(key("studio", studioToken), TTL_SESSION);
  return session;
}
async function enqueueJob(sessionId, type, payload) {
  const session = await getSession(sessionId);
  if (!session) throw new Error("Unknown session_id. Start a new session first.");
  if (!session.connected || !session.studioToken) throw new Error("Roblox Studio is not paired to this session yet.");
  const job = { id: randomId("job_"), type, payload, createdAt: new Date().toISOString() };
  await redis().rpush(key("queue", session.studioToken), job);
  await redis().expire(key("queue", session.studioToken), TTL_SESSION);
  return job;
}
async function popJob(studioToken) {
  return await redis().lpop(key("queue", studioToken));
}
async function putJobResult(studioToken, jobId, result) {
  const session = await touchStudio(studioToken);
  if (!session) return false;
  await redis().set(key("result", jobId), result, { ex: 60 * 10 });
  return true;
}
async function waitForJob(jobId, timeoutMs = 45000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const result = await redis().get(key("result", jobId));
    if (result !== null && result !== undefined) {
      await redis().del(key("result", jobId));
      return result;
    }
    await new Promise(r => setTimeout(r, 250));
  }
  throw new Error("Studio did not return a result before the timeout. Keep Studio open and the companion plugin connected, then retry.");
}
async function mirrorScript(sessionId, path, scriptType, source) {
  const session = await getSession(sessionId);
  if (!session) return;
  session.scripts ||= {};
  session.scripts[path] = { path, scriptType, source, updatedAt: new Date().toISOString() };
  await saveSession(session);
}
async function mirrorInstances(sessionId, items) {
  const session = await getSession(sessionId);
  if (!session) return;
  session.instances ||= {};
  for (const item of items) {
    session.instances[item.path] = {
      path: item.path, className: item.className,
      properties: item.properties || {}, updatedAt: new Date().toISOString()
    };
  }
  await saveSession(session);
}
async function mirrorDelete(sessionId, path) {
  const session = await getSession(sessionId);
  if (!session) return;
  session.scripts ||= {};
  session.instances ||= {};
  for (const p of Object.keys(session.scripts)) {
    if (p === path || p.startsWith(path + ".")) delete session.scripts[p];
  }
  for (const p of Object.keys(session.instances)) {
    if (p === path || p.startsWith(path + ".")) delete session.instances[p];
  }
  await saveSession(session);
}

function esc(s) {
  return String(s).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}
function cdata(s) {
  return String(s).replaceAll("]]>", "]]]]><![CDATA[>");
}
function ref() { return "RBX" + crypto.randomBytes(16).toString("hex"); }

const SERVICE_CLASS = {
  Workspace: "Workspace",
  Lighting: "Lighting",
  ReplicatedStorage: "ReplicatedStorage",
  ReplicatedFirst: "ReplicatedFirst",
  ServerScriptService: "ServerScriptService",
  ServerStorage: "ServerStorage",
  StarterGui: "StarterGui",
  StarterPack: "StarterPack",
  StarterPlayer: "StarterPlayer",
  SoundService: "SoundService",
  Teams: "Teams"
};

function pathParts(path) {
  const p = String(path || "").split(".").filter(Boolean);
  if (p[0]?.toLowerCase() === "game") p.shift();
  return p;
}
function typedProp(name, value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "boolean") return `<bool name="${esc(name)}">${value ? "true" : "false"}</bool>`;
  if (typeof value === "string") {
    if (name === "SoundId") return `<Content name="SoundId"><url>${esc(value)}</url></Content>`;
    return `<string name="${esc(name)}">${esc(value)}</string>`;
  }
  if (typeof value === "number") {
    const ints = new Set(["BorderSizePixel"]);
    const floats = new Set(["Transparency","Reflectance","Brightness","ClockTime","BackgroundTransparency","Volume","PlaybackSpeed"]);
    if (ints.has(name)) return `<int name="${esc(name)}">${Math.trunc(value)}</int>`;
    if (floats.has(name)) return `<float name="${esc(name)}">${value}</float>`;
    return "";
  }
  if (typeof value !== "object") return "";
  const t = value.$type;
  if (t === "Vector3") return `<Vector3 name="${esc(name)}"><X>${value.x || 0}</X><Y>${value.y || 0}</Y><Z>${value.z || 0}</Z></Vector3>`;
  if (t === "Vector2") return `<Vector2 name="${esc(name)}"><X>${value.x || 0}</X><Y>${value.y || 0}</Y></Vector2>`;
  if (t === "Color3") return `<Color3 name="${esc(name)}"><R>${value.r || 0}</R><G>${value.g || 0}</G><B>${value.b || 0}</B></Color3>`;
  if (t === "UDim2") return `<UDim2 name="${esc(name)}"><XS>${value.xs || 0}</XS><XO>${value.xo || 0}</XO><YS>${value.ys || 0}</YS><YO>${value.yo || 0}</YO></UDim2>`;
  if (t === "UDim") return `<UDim name="${esc(name)}"><S>${value.s || 0}</S><O>${value.o || 0}</O></UDim>`;
  if (t === "CFrame") {
    const rx = (value.rx || 0) * Math.PI / 180, ry = (value.ry || 0) * Math.PI / 180, rz = (value.rz || 0) * Math.PI / 180;
    const cx=Math.cos(rx), sx=Math.sin(rx), cy=Math.cos(ry), sy=Math.sin(ry), cz=Math.cos(rz), sz=Math.sin(rz);
    const r00=cy*cz, r01=cz*sx*sy-cx*sz, r02=sx*sz+cx*cz*sy;
    const r10=cy*sz, r11=cx*cz+sx*sy*sz, r12=cx*sy*sz-cz*sx;
    const r20=-sy, r21=cy*sx, r22=cx*cy;
    return `<CoordinateFrame name="${esc(name)}"><X>${value.x||0}</X><Y>${value.y||0}</Y><Z>${value.z||0}</Z><R00>${r00}</R00><R01>${r01}</R01><R02>${r02}</R02><R10>${r10}</R10><R11>${r11}</R11><R12>${r12}</R12><R20>${r20}</R20><R21>${r21}</R21><R22>${r22}</R22></CoordinateFrame>`;
  }
  return "";
}
function node(className, name, props = {}, children = [], source = null, scriptType = null) {
  const propertyXml = [];
  propertyXml.push(`<string name="Name">${esc(name)}</string>`);
  if (source !== null) {
    propertyXml.push(`<ProtectedString name="Source"><![CDATA[${cdata(source)}]]></ProtectedString>`);
    propertyXml.push(`<bool name="Disabled">false</bool>`);
    if (scriptType === "Script") propertyXml.push(`<token name="RunContext">1</token>`);
  }
  const allow = new Set(["Anchored","CanCollide","CanQuery","CanTouch","CastShadow","Transparency","Reflectance","Size","CFrame","Position","Color","Brightness","ClockTime","GlobalShadows","Enabled","ResetOnSpawn","IgnoreGuiInset","Text","TextScaled","Visible","BackgroundTransparency","BorderSizePixel","Volume","Looped","PlaybackSpeed","SoundId"]);
  for (const [k,v] of Object.entries(props || {})) if (allow.has(k)) propertyXml.push(typedProp(k, v));
  return `<Item class="${esc(className)}" referent="${ref()}"><Properties>${propertyXml.filter(Boolean).join("")}</Properties>${children.join("")}</Item>`;
}
function buildRbxlx(session) {
  const roots = new Map();
  const ensureService = (serviceName) => {
    if (!roots.has(serviceName)) roots.set(serviceName, { className: SERVICE_CLASS[serviceName] || serviceName, name: serviceName, props: {}, children: new Map(), scripts: new Map() });
    return roots.get(serviceName);
  };
  for (const item of Object.values(session.instances || {})) {
    const parts = pathParts(item.path);
    if (parts.length < 2) continue;
    let current = ensureService(parts[0]);
    for (let i=1; i<parts.length; i++) {
      const name = parts[i];
      if (!current.children.has(name)) current.children.set(name, { className: i === parts.length - 1 ? item.className : "Folder", name, props: {}, children: new Map(), scripts: new Map() });
      current = current.children.get(name);
    }
    current.className = item.className || current.className;
    current.props = item.properties || {};
  }
  for (const script of Object.values(session.scripts || {})) {
    const parts = pathParts(script.path);
    if (parts.length < 2) continue;
    let current = ensureService(parts[0]);
    for (let i=1; i<parts.length-1; i++) {
      const name = parts[i];
      if (!current.children.has(name)) current.children.set(name, { className: "Folder", name, props: {}, children: new Map(), scripts: new Map() });
      current = current.children.get(name);
    }
    current.scripts.set(parts.at(-1), script);
  }
  ensureService("Workspace");
  ensureService("ServerScriptService");
  const starterPlayer = ensureService("StarterPlayer");
  if (!starterPlayer.children.has("StarterPlayerScripts")) starterPlayer.children.set("StarterPlayerScripts", { className: "StarterPlayerScripts", name: "StarterPlayerScripts", props: {}, children: new Map(), scripts: new Map() });
  function render(n) {
    const children = [];
    for (const child of n.children.values()) children.push(render(child));
    for (const [name, s] of n.scripts.entries()) children.push(node(s.scriptType || "Script", name, {}, [], s.source || "", s.scriptType));
    return node(n.className, n.name, n.props, children);
  }
  const items = Array.from(roots.values()).map(render).join("");
  return `<?xml version="1.0" encoding="utf-8"?><roblox xmlns:xmime="http://www.w3.org/2005/05/xmlmime" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.roblox.com/roblox.xsd" version="4"><External>null</External><External>nil</External>${items}</roblox>`;
}

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}
function options(req, res) {
  if (req.method === "OPTIONS") {
    res.statusCode = 204;
    res.end();
    return true;
  }
  return false;
}
function bearer(req) {
  const h = String(req.headers.authorization || "");
  return h.startsWith("Bearer ") ? h.slice(7) : null;
}
async function healthHandler(req,res){ return json(res,200,{ok:true,service:"Roblox Studio Builder bridge"}); }
async function pairHandler(req, res) {
  if (options(req,res)) return;
  if (req.method !== "POST") return json(res,405,{error:"POST required"});
  const body = req.body || {};
  if (!body.code) return json(res,400,{error:"Missing pairing code"});
  const paired = await pairStudio(body.code, body.studioInfo || {});
  if (!paired) return json(res,404,{error:"Pairing code is invalid or expired"});
  return json(res,200,{ok:true,...paired});
}
async function pollHandler(req, res) {
  if (options(req,res)) return;
  if (req.method !== "GET") return json(res,405,{error:"GET required"});
  const token = bearer(req);
  if (!token) return json(res,401,{error:"Missing bearer token"});
  const session = await touchStudio(token);
  if (!session) return json(res,401,{error:"Invalid Studio token"});
  const job = await popJob(token);
  return json(res,200,{ok:true,job:job || null,sessionId:session.sessionId});
}
async function resultHandler(req, res) {
  if (options(req,res)) return;
  if (req.method !== "POST") return json(res,405,{error:"POST required"});
  const token = bearer(req);
  if (!token) return json(res,401,{error:"Missing bearer token"});
  const body = req.body || {};
  if (!body.jobId) return json(res,400,{error:"Missing jobId"});
  const success = await putJobResult(token, body.jobId, body.result ?? {ok:true});
  if (!success) return json(res,401,{error:"Invalid Studio token"});
  return json(res,200,{ok:true});
}
async function downloadHandler(req,res){
  const sessionId = req.query.sessionId;
  const session = await getSession(sessionId);
  if (!session) { res.statusCode=404; return res.end("Unknown or expired build session"); }
  const xml = buildRbxlx(session);
  const safe = String(session.projectName || "RobloxGame").replace(/[^a-z0-9-_]+/gi,"_").slice(0,80) || "RobloxGame";
  res.statusCode=200;
  res.setHeader("Content-Type","application/xml; charset=utf-8");
  res.setHeader("Content-Disposition",`attachment; filename="${safe}.rbxlx"`);
  res.end(xml);
}

const ok = (data) => ({ content:[{type:"text",text:JSON.stringify(data)}], structuredContent:data });
const fail = (error) => ({ content:[{type:"text",text:`Error: ${error instanceof Error ? error.message : String(error)}`}], isError:true });
const baseUrl = () => (process.env.PUBLIC_BASE_URL || "http://localhost:3000").replace(/\/$/,"");

function makeServer(){
  const server = new McpServer(
    { name:"roblox-studio-game-builder", version:"1.0.0" },
    { instructions:"For complete game requests: start and pair a Studio session, build real scripts and instances, inspect the actual game tree, playtest, repair every blocking error, polish, playtest again, then export. Never claim a test or export succeeded without a tool result." }
  );

  server.registerTool("roblox_start_session", {
    title:"Start Roblox build session",
    description:"Start a new Roblox Studio build session and get a 6-digit pairing code for the Studio companion plugin. Call this first for a new game/build.",
    inputSchema:{ project_name:z.string().min(1).max(100).default("Roblox Game") }
  }, async ({project_name}) => {
    try { const s=await createSession(project_name); return ok({session_id:s.sessionId,pairing_code:s.code,studio_connected:false,instructions:"Open the Roblox Studio companion plugin, enter this code, and press Connect."}); }
    catch(e){ return fail(e); }
  });

  server.registerTool("roblox_connection_status", {
    title:"Check Roblox Studio connection",
    description:"Check whether Roblox Studio is paired and recently reachable for a build session.",
    inputSchema:{ session_id:z.string() }
  }, async ({session_id}) => {
    try { const s=await getSession(session_id); if(!s) throw new Error("Unknown or expired session_id"); return ok({connected:!!s.connected,last_seen_at:s.lastSeenAt,studio_info:s.studioInfo||null,project_name:s.projectName}); }
    catch(e){ return fail(e); }
  });

  server.registerTool("roblox_write_script", {
    title:"Write full Roblox script",
    description:"Create or fully replace a Script, LocalScript, or ModuleScript in the paired Studio place. Use complete source code, not a partial patch.",
    inputSchema:{ session_id:z.string(), path:z.string().describe("Dot path such as game.ServerScriptService.Main"), script_type:z.enum(["Script","LocalScript","ModuleScript"]), source:z.string() }
  }, async ({session_id,path,script_type,source}) => {
    try {
      const job=await enqueueJob(session_id,"set_script",{path,scriptType:script_type,source});
      const result=await waitForJob(job.id,50000);
      if(!result?.ok) throw new Error(result?.error || "Studio rejected script update");
      await mirrorScript(session_id,path,script_type,source);
      return ok({ok:true,path,script_type,studio_result:result});
    } catch(e){ return fail(e); }
  });

  server.registerTool("roblox_read_script", {
    title:"Read Roblox script",
    description:"Read the current full source of a script from the paired Studio place before repairing or upgrading it.",
    inputSchema:{ session_id:z.string(), path:z.string() }
  }, async ({session_id,path}) => {
    try { const job=await enqueueJob(session_id,"read_script",{path}); const result=await waitForJob(job.id); if(!result?.ok) throw new Error(result?.error||"Read failed"); return ok(result); }
    catch(e){ return fail(e); }
  });

  server.registerTool("roblox_apply_instances", {
    title:"Create or update Roblox instances",
    description:"Create or update edit-time Roblox instances in Studio. Each item has path, className and properties. Supports typed Vector3, Vector2, Color3, CFrame, UDim, UDim2, NumberRange, BrickColor and Enum values.",
    inputSchema:{ session_id:z.string(), items:z.array(z.object({ path:z.string(), className:z.string(), properties:z.record(z.string(),z.any()).optional() })).min(1).max(300) }
  }, async ({session_id,items}) => {
    try { const job=await enqueueJob(session_id,"apply_instances",{items}); const result=await waitForJob(job.id,50000); if(!result?.ok) throw new Error(result?.error||"Instance batch failed"); await mirrorInstances(session_id,items); return ok({ok:true,count:items.length,studio_result:result}); }
    catch(e){ return fail(e); }
  });

  server.registerTool("roblox_delete", {
    title:"Delete Roblox instance tree",
    description:"Delete an instance/script path from the paired place and its descendants.",
    inputSchema:{ session_id:z.string(), path:z.string() }
  }, async ({session_id,path}) => {
    try { const job=await enqueueJob(session_id,"delete",{path}); const result=await waitForJob(job.id); if(!result?.ok) throw new Error(result?.error||"Delete failed"); await mirrorDelete(session_id,path); return ok(result); }
    catch(e){ return fail(e); }
  });

  server.registerTool("roblox_inspect_tree", {
    title:"Inspect Roblox game tree",
    description:"Read the actual instance hierarchy from the open paired Studio place. Use after major build steps and before repairs.",
    inputSchema:{ session_id:z.string(), root_path:z.string().default("game"), max_depth:z.number().int().min(1).max(8).default(4), max_items:z.number().int().min(10).max(1200).default(400) }
  }, async ({session_id,root_path,max_depth,max_items}) => {
    try { const job=await enqueueJob(session_id,"get_tree",{rootPath:root_path,maxDepth:max_depth,maxItems:max_items}); const result=await waitForJob(job.id); if(!result?.ok) throw new Error(result?.error||"Tree inspection failed"); return ok(result); }
    catch(e){ return fail(e); }
  });

  server.registerTool("roblox_inspect_instance", {
    title:"Inspect Roblox instance",
    description:"Inspect a specific instance and a set of common readable properties from Studio.",
    inputSchema:{ session_id:z.string(), path:z.string() }
  }, async ({session_id,path}) => {
    try { const job=await enqueueJob(session_id,"inspect_instance",{path}); const result=await waitForJob(job.id); if(!result?.ok) throw new Error(result?.error||"Inspection failed"); return ok(result); }
    catch(e){ return fail(e); }
  });

  server.registerTool("roblox_playtest", {
    title:"Playtest Roblox game",
    description:"Run an automated solo Studio playtest, capture output/errors, stop automatically, and return the log. Use after building and after every important fix.",
    inputSchema:{ session_id:z.string(), seconds:z.number().min(2).max(25).default(8) }
  }, async ({session_id,seconds}) => {
    try { const job=await enqueueJob(session_id,"playtest",{seconds}); const result=await waitForJob(job.id,55000); if(!result?.ok) throw new Error(result?.error||"Playtest failed"); return ok(result); }
    catch(e){ return fail(e); }
  });

  server.registerTool("roblox_export_place", {
    title:"Export Roblox place file",
    description:"Create a downloadable .rbxlx Roblox place from the session's latest successfully-written scripts and supported edit-time instances. Call only after playtesting and fixing blocking errors.",
    inputSchema:{ session_id:z.string() }
  }, async ({session_id}) => {
    try { const s=await getSession(session_id); if(!s) throw new Error("Unknown or expired session_id"); const url=`${baseUrl()}/download/${encodeURIComponent(session_id)}`; return ok({ok:true,format:"rbxlx",download_url:url,project_name:s.projectName,note:"Open the .rbxlx directly in Roblox Studio. Studio can then save it as binary .rbxl if desired."}); }
    catch(e){ return fail(e); }
  });

  return server;
}

async function mcpHandler(req,res){
  if(req.method==="OPTIONS"){res.statusCode=204; return res.end();}
  if(req.method!=="POST"){
    res.statusCode=405; res.setHeader("Content-Type","application/json");
    return res.end(JSON.stringify({jsonrpc:"2.0",error:{code:-32000,message:"POST required for stateless MCP"},id:null}));
  }
  const server=makeServer();
  const transport=new StreamableHTTPServerTransport({sessionIdGenerator:undefined});
  try{
    await server.connect(transport);
    await transport.handleRequest(req,res,req.body);
  }catch(error){
    console.error("MCP error", error);
    if(!res.headersSent){res.statusCode=500;res.setHeader("Content-Type","application/json");res.end(JSON.stringify({jsonrpc:"2.0",error:{code:-32603,message:"Internal MCP error"},id:null}));}
  }finally{
    try{await transport.close();}catch{}
    try{await server.close();}catch{}
  }
}

const PORT = Number(process.env.PORT || 3000);
const MAX_BODY = 8 * 1024 * 1024;
function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, Mcp-Session-Id, Last-Event-ID");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
}
async function parseBody(req) {
  if (!["POST","PUT","PATCH"].includes(req.method || "")) return undefined;
  const type=String(req.headers["content-type"]||"");
  if (!type.includes("application/json")) return undefined;
  let size=0; const chunks=[];
  for await (const chunk of req) { size+=chunk.length; if(size>MAX_BODY) throw new Error("Request body too large"); chunks.push(chunk); }
  if(!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
const server=http.createServer(async(req,res)=>{
  setCors(res);
  if(req.method==="OPTIONS"){res.statusCode=204;return res.end();}
  try{
    const url=new URL(req.url||"/",`http://${req.headers.host||"localhost"}`);
    req.body=await parseBody(req);
    req.query=Object.fromEntries(url.searchParams.entries());
    if(url.pathname==="/"||url.pathname==="/health"){
      res.statusCode=200;
      res.setHeader("Content-Type","application/json; charset=utf-8");
      return res.end(JSON.stringify({ok:true,service:"Roblox Studio Game Builder",mcp:"/mcp",studioHealth:"/api/studio/health"}));
    }
    if(url.pathname==="/mcp") return await mcpHandler(req,res);
    if(url.pathname==="/studio/health"||url.pathname==="/api/studio/health") return await healthHandler(req,res);
    if(url.pathname==="/studio/pair"||url.pathname==="/api/studio/pair") return await pairHandler(req,res);
    if(url.pathname==="/studio/poll"||url.pathname==="/api/studio/poll") return await pollHandler(req,res);
    if(url.pathname==="/studio/result"||url.pathname==="/api/studio/result") return await resultHandler(req,res);
    const match=url.pathname.match(/^\/download\/([^/]+)$/);
    if(match){req.query.sessionId=decodeURIComponent(match[1]);return await downloadHandler(req,res);}
    res.statusCode=404;
    res.setHeader("Content-Type","application/json; charset=utf-8");
    return res.end(JSON.stringify({error:"Not found"}));
  }catch(error){
    console.error(error);
    if(!res.headersSent){
      res.statusCode=error?.message==="Request body too large"?413:500;
      res.setHeader("Content-Type","application/json; charset=utf-8");
    }
    if(!res.writableEnded) res.end(JSON.stringify({error:error?.message||"Internal server error"}));
  }
});
server.listen(PORT,"0.0.0.0",()=>console.log(`Roblox Studio Game Builder listening on port ${PORT}`));
