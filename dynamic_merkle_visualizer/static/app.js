"use strict";
const $ = id => document.getElementById(id);
const state = {catalog:null, snapshot:null, previous:null, partition:0, focus:null, node:null, row:null, offset:0, rowScope:null, busy:false, writable:false, sequence:0, rowsSequence:0};
const svgNS = "http://www.w3.org/2000/svg";
function text(id, value) { $(id).textContent = value ?? "—"; }
function message(value, error=false) { text("message", value); $("message").className = error ? "error" : ""; }
function el(tag, content, className) { const e=document.createElement(tag); if(content!==undefined)e.textContent=content; if(className)e.className=className; return e; }
async function api(path, params={}, body=null) {
  const options = body ? {method:"POST", headers:{"Content-Type":"application/json", "X-Merkle-Token":state.catalog.token}, body:JSON.stringify(body)} : {};
  const response = await fetch(path + (body ? "" : "?" + new URLSearchParams(params)), options);
  const result = await response.json();
  if(!response.ok) throw new Error(result.error || `HTTP ${response.status}`);
  return result;
}
function selectedIndex() { return $("index-select").value; }
function setBusy(busy) {
  state.busy=busy;
  for(const id of ["refresh","verify","index-select","partition-root","operation","values","rollback"]) $(id).disabled=busy;
  $("submit").disabled=busy || !state.writable || !state.snapshot;
  $("focus").disabled=busy || !state.node;
  $("partitions").querySelectorAll("button").forEach(b=>b.disabled=busy);
  document.querySelector("main").classList.toggle("busy",busy);
}
async function boot() {
  try {
    state.catalog=await api("/api/catalog");
    const c=state.catalog;
    state.writable=c.allow_writes && c.database.superuser;
    $("index-select").replaceChildren();
    for(const index of c.indexes) {
      const option=el("option",`${index.table_schema}.${index.table_name} → ${index.index_name}`);
      option.value=index.index_oid; $("index-select").append(option);
    }
    const d=c.database;
    text("connection-detail",`${d.database} · ${d.role} · ${d.address || "Unix socket"}${d.port ? ":"+d.port : ""}`);
    $("connection-detail").title=d.version;
    text("connection-status","Connected to PostgreSQL"); $("connection-status").className="badge live";
    text("write-mode",state.writable ? "Writes enabled" : "Read only");
    text("editor-mode",state.writable ? "Native DML" : c.allow_writes ? "Superuser required" : "Writes disabled");
    $("submit").disabled=!state.writable;
    if(!c.indexes.length) { state.snapshot=null; setBusy(false); throw new Error("No Merkle index matches this connection. Use the demo launcher, or configure an existing current Merkle index."); }
    await refresh();
  } catch(error) {
    message(error.message,true); text("connection-status","Connection needs attention"); $("connection-status").className="badge error";
    $("submit").disabled=true;
  }
}
async function refresh(verify=false) {
  if(state.busy) return;
  if(!selectedIndex()) return boot();
  setBusy(true); message(verify ? "PostgreSQL is scanning the heap and checking its aggregate…" : "Reading a locked database snapshot…");
  const sequence=++state.sequence;
  try {
    const params={index_oid:selectedIndex(),partition:state.partition,verify:verify?"1":"0"};
    if(state.focus) Object.assign(params,state.focus);
    const snapshot=await api("/api/snapshot",params);
    if(sequence!==state.sequence)return;
    state.previous=state.snapshot?.target.index_oid===snapshot.target.index_oid ? state.snapshot : null;
    state.snapshot=snapshot;
    if(state.node) state.node=snapshot.nodes.find(n=>n.id===state.node.id) || null;
    // A refresh invalidates an edit selection rather than quietly retargeting it.
    state.row=null; state.offset=0;
    if(state.rowScope && !snapshot.nodes.some(n=>n.node_id===state.rowScope.node_id && n.prefix_len===state.rowScope.prefix_len)) state.rowScope=null;
    drawSnapshot();
    await loadRows();
    message(`Snapshot complete · ${snapshot.elapsed_ms} ms${snapshot.truncated ? " · node view is truncated; focus a subtree" : ""}`);
  } catch(error) { message(error.message,true); }
  finally { setBusy(false); }
}
function drawSnapshot() {
  const s=state.snapshot;
  text("format",`v${s.stats.version}`); text("hash-format",`Route ${s.stats.route_format_version} · row hash ${s.stats.row_hash_format_version}`);
  text("node-count",s.stats.total_nodes.toLocaleString()); text("leaf-count",`${s.stats.leaf_nodes.toLocaleString()} stored leaves across all partitions`);
  text("geometry",`${s.stats.fanout}-way / ${s.stats.partitions} partitions`); text("thresholds",`Split ${s.stats.split_threshold} · merge ${s.stats.merge_threshold}`);
  text("audit",s.verification===null ? "Not run" : s.verification ? "PASS" : "MISMATCH");
  $("audit").className=s.verification===null ? "" : s.verification ? "pass" : "fail";
  text("audit-detail",s.verification===null ? "Not audited in this snapshot" : "Native heap aggregate vs stored roots");
  text("global-root",s.root); text("snapshot-time",`${new Date(s.snapshot_at).toLocaleTimeString()} · ${s.elapsed_ms} ms`);
  text("partition-count",s.partitions.length);
  $("partitions").replaceChildren();
  for(const partition of s.partitions) {
    const previous=state.previous?.partitions.find(p=>p.partition===partition.partition);
    const button=el("button",undefined,"partition"+(partition.partition===state.partition?" selected":"")+(previous && previous.hash!==partition.hash?" changed":""));
    button.append(el("span",`P${partition.partition}`),el("code",partition.hash.slice(0,8))); button.title=partition.hash;
    button.addEventListener("click",()=>{if(state.busy)return; state.partition=partition.partition; state.focus=null; state.node=null;state.rowScope=null;refresh();});
    $("partitions").append(button);
  }
  text("tree-title",`Partition ${state.partition}${s.focus.prefix_len ? " · /"+s.focus.prefix_len+" subtree" : " · native prefix tree"}`);
  text("tree-notice",s.truncated ? `Showing the first ${s.node_limit} stored nodes. Focus a selected prefix to inspect deeper nodes. No missing nodes are invented.` : `${s.nodes.length} stored nodes in this view. Scroll to explore wide trees.`);
  text("row-capability",s.leaf_rows_reason || "Node filtering uses PostgreSQL's native key hash, partition, and prefix-bound functions.");
  text("storage-evidence",`Source: ariabc_internal.merkle_node_${s.target.index_oid} · ${s.target.definition}`);
  text("settings-evidence",`Reader session: maintenance ${s.settings.maintenance}; direct apply ${s.settings.direct_apply}; synchronous_commit ${s.settings.synchronous_commit}; fsync ${s.settings.fsync}. Inspector writes explicitly enable maintenance and synchronous commit.`);
  text("stats-json",JSON.stringify(s.stats,null,2));
  drawTree(); drawNodeTable(); drawDetail();
  if(!$("values").value) {
    const defaults={};
    for(const column of s.columns.filter(c=>!c.generated && c.identity!=="a")) defaults[column.name]=column.not_null ? "" : null;
    $("values").value=JSON.stringify(defaults,null,2);
  }
  text("row-selection","No current row selected. Select a row before UPDATE or DELETE.");
}
function nodeChanged(node) {
  if(state.previous?.partition!==state.partition)return false;
  const old=state.previous?.nodes.find(n=>n.id===node.id);
  return !!state.previous && (!old || old.hash!==node.hash || old.tuple_count!==node.tuple_count || old.is_leaf!==node.is_leaf);
}
function svg(tag,attrs={},content) {
  const element=document.createElementNS(svgNS,tag);
  for(const [k,v] of Object.entries(attrs))element.setAttribute(k,String(v));
  if(content!==undefined)element.textContent=content;
  return element;
}
function drawTree() {
  const tree=$("tree"); tree.replaceChildren();
  const nodes=state.snapshot.nodes;
  $("tree-empty").hidden=nodes.length>0;
  if(!nodes.length){tree.setAttribute("width",0);tree.setAttribute("height",0);return;}
  const mapped=new Map(nodes.map(n=>[n.id,{...n,children:[]}]))
  const roots=[];
  for(const node of mapped.values()) { if(node.parent && mapped.has(node.parent))mapped.get(node.parent).children.push(node); else roots.push(node); }
  let cursor=0,maxDepth=0;
  function position(node,depth) {
    node.depth=depth;maxDepth=Math.max(maxDepth,depth);
    for(const child of node.children)position(child,depth+1);
    node.x=node.children.length ? (node.children[0].x+node.children.at(-1).x)/2 : 86+cursor++*165;
    node.y=45+depth*112;
  }
  roots.forEach(n=>position(n,0));
  const width=Math.max(340,cursor*165+15),height=Math.max(180,(maxDepth+1)*112+30);
  tree.setAttribute("width",width);tree.setAttribute("height",height);tree.setAttribute("viewBox",`0 0 ${width} ${height}`);
  for(const node of mapped.values())for(const child of node.children)tree.append(svg("path",{class:"tree-link",d:`M ${node.x} ${node.y+32} C ${node.x} ${node.y+67},${child.x} ${child.y-65},${child.x} ${child.y-32}`}));
  for(const node of mapped.values()) {
    const selected=state.node?.id===node.id;
    const group=svg("g",{class:`tree-node ${node.is_leaf?"leaf":"internal"}${nodeChanged(node)?" changed":""}${selected?" selected":""}`,transform:`translate(${node.x},${node.y})`,tabindex:"0",role:"button","aria-label":`${node.is_leaf?"Leaf":"Internal"} ${node.node_id} prefix ${node.prefix_len}, ${node.tuple_count} rows`});
    group.append(svg("rect",{x:-72,y:-32,width:144,height:66,rx:7}));
    group.append(svg("text",{x:0,y:-12,"text-anchor":"middle"},`${node.prefix_len===0?"root":node.node_id.slice(0,8)} /${node.prefix_len}`));
    group.append(svg("text",{x:0,y:6,"text-anchor":"middle"},`${node.is_leaf?"leaf":"branch"} · ${node.tuple_count} rows`));
    group.append(svg("text",{x:0,y:23,"text-anchor":"middle",class:"hash-label"},node.hash.slice(0,14)));
    group.append(svg("title",{},`Stored prefix: ${node.node_id}/${node.prefix_len}\nHash: ${node.hash}\nRows: ${node.tuple_count}`));
    group.addEventListener("click",()=>selectNode(node.id));
    group.addEventListener("keydown",event=>{if(event.key==="Enter" || event.key===" "){event.preventDefault();selectNode(node.id);}});
    tree.append(group);
  }
}
function drawNodeTable() {
  const table=$("node-table"); table.replaceChildren();
  const head=el("thead"),header=el("tr");
  ["Prefix","Bits","Kind","Rows","Stored hash"].forEach(label=>header.append(el("th",label)));head.append(header);table.append(head);
  const body=el("tbody");
  for(const node of state.snapshot.nodes){const row=el("tr");[node.node_id,node.prefix_len,node.is_leaf?"leaf":"internal",node.tuple_count,node.hash].forEach(value=>{const cell=el("td",value);cell.title=String(value);row.append(cell);});row.addEventListener("click",()=>selectNode(node.id));body.append(row);}
  table.append(body);
}
function drawDetail() {
  const detail=$("node-detail");detail.replaceChildren();
  const n=state.node;
  const entries=n ? [["Kind",n.is_leaf?"Leaf":"Internal"],["Partition",n.partition_id],["Prefix",`${n.node_id} / ${n.prefix_len} bits`],["Tuple count",n.tuple_count],["Stored hash",n.hash],["Parent in view",n.parent || "None"]] : [["Selection","Choose a stored node."]];
  entries.forEach(([key,value])=>detail.append(el("dt",key),el("dd",value)));
  $("focus").disabled=state.busy || !n;
}
async function selectNode(id) {
  if(state.busy)return;
  state.node=state.snapshot.nodes.find(n=>n.id===id);state.offset=0;state.row=null;
  state.rowScope=state.snapshot.leaf_rows_supported ? {partition:state.partition,node_id:state.node.node_id,prefix_len:state.node.prefix_len} : null;
  drawTree();drawDetail();await loadRows();
}
async function loadRows() {
  const sequence=++state.rowsSequence;
  try {
    const params={index_oid:selectedIndex(),offset:state.offset,...(state.rowScope || {})};
    const result=await api("/api/rows",params);
    if(sequence!==state.rowsSequence)return;
    const table=$("rows-table");table.replaceChildren();
    const head=el("thead"),header=el("tr");
    ["Edit",...state.snapshot.columns.map(c=>c.name),"Native row hash"].forEach(label=>header.append(el("th",label)));head.append(header);table.append(head);
    const body=el("tbody");
    for(const record of result.rows){
      const row=el("tr");const action=el("td"),button=el("button","Select");action.append(button);row.append(action);
      for(const column of state.snapshot.columns){const value=record.values[column.name];const cell=el("td",value===null?"NULL":value);cell.title=value===null?"SQL NULL":value;row.append(cell);}
      const hash=el("td",record.hash.slice(0,16));hash.title=record.hash;row.append(hash);
      button.addEventListener("click",()=>{if(state.busy)return;state.row=record;$("operation").value="update";const values={};for(const c of state.snapshot.columns.filter(c=>!c.generated && c.identity!=="a"))values[c.name]=record.values[c.name];$("values").value=JSON.stringify(values,null,2);text("row-selection",`Selected ctid ${record.ctid} · xmin ${record.xmin}. Stale edits are rejected.`);body.querySelectorAll("tr").forEach(r=>r.classList.remove("selected"));row.classList.add("selected");});
      body.append(row);
    }
    table.append(body);
    text("rows-scope",`${result.scope}${state.rowScope?` · P${state.partition} /${state.rowScope.prefix_len}`:""} · read ${new Date(result.snapshot_at).toLocaleTimeString()}`);
    text("row-page",`${result.rows.length ? result.offset+1 : 0}–${result.offset+result.rows.length} · ${result.has_more?"more rows available":"end of this view"}`);
    $("previous").disabled=result.offset===0;$("next").disabled=!result.has_more;
  } catch(error){if(sequence===state.rowsSequence){$("rows-table").replaceChildren();message(error.message,true);}}
}
async function mutate() {
  if(state.busy)return;
  const operation=$("operation").value;
  let values;
  try{values=operation==="delete"?{}:JSON.parse($("values").value);}catch{message("Enter valid JSON column values. Use strings to preserve PostgreSQL types and large integers.",true);return;}
  if(operation!=="insert" && !state.row){message("Select a current row before UPDATE or DELETE.",true);return;}
  const request={index_oid:selectedIndex(),operation,values,row:state.row,rollback:$("rollback").checked};
  setBusy(true);text("transaction-result","Waiting for PostgreSQL transaction completion…");
  try {
    const result=await api("/api/mutate",{},request);
    text("transaction-result",`${result.message}. ${result.affected_rows} row affected${result.rolled_back?" before rollback":""}.`);
    state.row=null;state.node=null;state.focus=null;state.rowScope=null;
    setBusy(false);
    await refresh();
  } catch(error){text("transaction-result",error.message);message(error.message,true);}
  finally{setBusy(false);}
}
$("refresh").addEventListener("click",()=>refresh());
$("verify").addEventListener("click",()=>refresh(true));
$("index-select").addEventListener("change",()=>{state.snapshot=null;state.previous=null;state.partition=0;state.focus=null;state.rowScope=null;state.row=null;state.node=null;$("values").value="";refresh();});
$("partition-root").addEventListener("click",()=>{state.focus=null;state.node=null;state.rowScope=null;refresh();});
$("focus").addEventListener("click",()=>{if(!state.node)return;state.focus={node_id:state.node.node_id,prefix_len:state.node.prefix_len};refresh();});
$("all-rows").addEventListener("click",()=>{if(state.busy)return;state.rowScope=null;state.offset=0;loadRows();});
$("previous").addEventListener("click",()=>{if(state.busy)return;state.offset=Math.max(0,state.offset-state.catalog.row_limit);loadRows();});
$("next").addEventListener("click",()=>{if(state.busy)return;state.offset+=state.catalog.row_limit;loadRows();});
$("operation").addEventListener("change",()=>{$("values").disabled=$("operation").value==="delete";});
$("submit").addEventListener("click",mutate);
setInterval(()=>{if($("auto-refresh").checked && !state.busy && !state.row && document.visibilityState==="visible")refresh();},5000);
boot();
