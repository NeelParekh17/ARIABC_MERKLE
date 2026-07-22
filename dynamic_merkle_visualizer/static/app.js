const $ = id => document.getElementById(id);
let snapshot = null;
let timeline = [];
let selectedNode = null;
let leafRequest = 0;
let lastPlayback = null;
let logicalTreeCache = new Map();
const initialView = new URLSearchParams(location.search);

async function api(path, body) {
  const response = await fetch(path, {
    method: body ? 'POST' : 'GET',
    headers: body ? {'Content-Type': 'application/json'} : {},
    body: body ? JSON.stringify(body) : null
  });
  const result = await response.json();
  if (!response.ok) throw Error(result.error || response.statusText);
  return result;
}

const wait = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

function toast(message, bad = false) {
  const element = $('toast');
  element.textContent = message;
  element.style.borderColor = bad ? 'var(--red)' : 'var(--cyan)';
  element.style.display = 'block';
  setTimeout(() => element.style.display = 'none', 4500);
}

function config() {
  return {
    partitions: +$('partitions').value,
    leaves_per_partition: +$('initialLeaves').value,
    fanout: 32,
    leaf_capacity: +$('leafCapacity').value,
    merge_threshold: +$('mergeThreshold').value,
    leaf_byte_capacity: +$('leafBytes').value,
    max_key_bytes: +$('maxKeyBytes').value,
    update_mode: 'synchronous_cow'
  };
}

async function fileText(id) {
  const file = $(id).files[0];
  if (!file) throw Error(`Choose a ${id} file first`);
  return [file, await file.text()];
}

function metric(name, value) {
  return `<div class="metric"><b>${value ?? '—'}</b><span>${name}</span></div>`;
}

function leafId(leaf) {
  const bits = leaf.prefix_bits || '';
  return `p${leaf.partition_id}:${leaf.prefix_len}:${bits}`;
}

function commonPrefix(a = '', b = '') {
  let length = 0;
  while (length < a.length && length < b.length && a[length] === b[length]) length++;
  return length;
}

function closestSelectedNode(nodes) {
  if (!selectedNode) return null;
  const exact = nodes.find(node => node.id === selectedNode.id);
  if (exact) return exact;
  const samePartition = nodes.filter(node => node.partition_id === selectedNode.partition_id);
  if (!samePartition.length) return null;
  return samePartition.sort((a, b) => {
    const aRelated = a.prefix_bits.startsWith(selectedNode.prefix_bits) || selectedNode.prefix_bits.startsWith(a.prefix_bits);
    const bRelated = b.prefix_bits.startsWith(selectedNode.prefix_bits) || selectedNode.prefix_bits.startsWith(b.prefix_bits);
    if (aRelated !== bRelated) return bRelated - aRelated;
    return commonPrefix(b.prefix_bits, selectedNode.prefix_bits) - commonPrefix(a.prefix_bits, selectedNode.prefix_bits) ||
           Math.abs(a.prefix_len - selectedNode.prefix_len) - Math.abs(b.prefix_len - selectedNode.prefix_len);
  })[0];
}

function updateSnapshotChrome(value) {
  const stats = value.stats || {};
  $('health').textContent = value.verify ? 'Verified native index' : stats.state || 'Not built';
  $('health').style.color = value.verify ? 'var(--green)' : 'var(--muted)';
  const internalNodes = stats.node_count == null || stats.leaf_count == null ? null :
    Number(stats.node_count) - Number(stats.leaf_count);
  $('metrics').innerHTML = metric('Rows', value.row_count) + metric('Physical nodes', stats.node_count) +
    metric('Physical leaves', stats.leaf_count) + metric('Physical internal', internalNodes) + metric('Physical depth', stats.max_depth) +
    metric('Max leaf rows', stats.max_leaf_items) + metric('Splits', stats.split_count) +
    metric('Merges', stats.merge_count);
  $('contract').innerHTML = [
    ['Authority', stats.authority], ['Layout', `v${stats.layout_version ?? '—'}`],
    ['Logical fanout', stats.logical_fanout], ['Logical bits / level', stats.logical_fanout === 32 ? 5 : '—'],
    ['Physical fanout', stats.physical_node_fanout], ['Update mode', stats.update_mode]
  ].map(([name, value]) => `<div><span>${name}</span><b>${value ?? '—'}</b></div>`).join('');
  $('rootHashes').innerHTML = [
    ['Combined root', stats.combined_root], ['Data root', stats.data_root],
    ['Structure root', stats.structure_root]
  ].map(([name, hash]) => `<div><b>${name}</b><code title="${hash || ''}">${hash || '—'}</code></div>`).join('');

  const partitions = [...new Set((value.nodes || []).map(node => node.partition_id))];
  const oldPartition = $('partition').value;
  $('partition').innerHTML = partitions.map(partition => `<option>${partition}</option>`).join('');
  if (oldPartition !== '' && partitions.includes(+oldPartition)) $('partition').value = oldPartition;
  else if (partitions.includes(+initialView.get('partition'))) $('partition').value = initialView.get('partition');
  else if (selectedNode && partitions.includes(selectedNode.partition_id)) $('partition').value = selectedNode.partition_id;
}

async function render(value, transition = null) {
  const before = snapshot;
  const normalized = transition ? normalizeTransition(transition, before, value) : null;
  if (normalized) {
    lastPlayback = {transition: normalized, before, after: value};
    $('replayTransition').disabled = false;
    await playTransition(normalized, value, false);
  } else {
    snapshot = value;
    logicalTreeCache.clear();
    updateSnapshotChrome(value);
    await renderTree(true);
  }
}

function nodeMarkup(node, mode) {
  const logical = node.node_kind === 'logical_range';
  const terminal = logical ? node.bounded : node.is_leaf;
  const type = logical ? (node.empty ? 'empty logical slot' : node.bounded ? 'bounded logical range' : 'logical range') :
    (node.is_leaf ? 'physical leaf' : 'physical internal');
  const depth = logical ? `logical level ${node.logical_level}` : `physical depth ${node.physical_depth}`;
  const slot = logical && node.slot != null ? ` · slot ${node.slot}` : '';
  return `<div class="node ${terminal ? 'leaf' : 'internal'} ${logical ? 'logical' : 'physical'} ${node.empty ? 'empty' : ''}" data-id="${node.id}">
    <div class="type">${type} · ${depth}${slot}</div>
    <div class="count">${node.tuple_count} rows</div><div>${node.prefix_bits}</div>
    <div class="hash">${mode === 'structure_hash' ? 'S' : 'D'} ${node[mode]}</div>
    ${!logical && node.is_leaf ? '<button class="node-insert">+ Insert row</button>' : ''}</div>`;
}

async function treeNodes(partition) {
  if ($('treeView').value === 'physical') return {nodes: snapshot.nodes || [], summary: null};
  const includeEmpty = $('showEmptyLogical').checked;
  const key = `${partition}:${includeEmpty}`;
  if (!logicalTreeCache.has(key)) {
    logicalTreeCache.set(key, await api(`/api/logical-tree?partition=${partition}&include_empty=${includeEmpty}`));
  }
  return logicalTreeCache.get(key);
}

async function renderTree(animate = false, transition = null) {
  if (!snapshot) return;
  const tree = $('tree');
  const oldPositions = new Map([...tree.querySelectorAll('.node')].map(node => [node.dataset.id, node.getBoundingClientRect()]));
  const partition = +$('partition').value;
  const mode = $('hashMode').value;
  const scale = +$('zoom').value / 100;
  const treeData = await treeNodes(partition);
  const nodes = treeData.nodes.filter(node => node.partition_id === partition);
  const logical = $('treeView').value === 'logical';
  $('showEmptyLogical').disabled = !logical;
  $('viewSummary').textContent = logical ?
    `Query-time localisation geometry: fanout ${treeData.summary.logical_fanout}, ${treeData.summary.bits_per_level} route bits per level, ${treeData.summary.range_count} displayed ranges (${treeData.summary.nonempty_range_count} non-empty, ${treeData.summary.bounded_range_count} bounded), ${treeData.summary.levels} logical levels. These ranges are not stored nodes.` :
    `Authoritative stored topology: binary fanout ${snapshot.stats.physical_node_fanout}, ${snapshot.stats.node_count} physical nodes reconstructed exactly from ${snapshot.stats.leaf_count} frontier leaves.`;
  const levels = {};
  nodes.forEach(node => (levels[node.prefix_len] ??= []).push(node));
  tree.style.transformOrigin = 'top center';
  tree.style.transform = `scale(${scale})`;
  tree.innerHTML = '<svg id="treeEdges" class="tree-edges" aria-hidden="true"></svg>' +
    Object.keys(levels).sort((a, b) => a - b).map(level =>
    `<div class="tree-level">${levels[level].map(node => nodeMarkup(node, mode)).join('')}</div>`
  ).join('') || '<p>No nodes in this partition.</p>';

  tree.querySelectorAll('.node').forEach(element => {
    const node = nodes.find(candidate => candidate.id === element.dataset.id);
    element.onclick = () => selectNode(node, element);
    const insertButton = element.querySelector('.node-insert');
    if (insertButton) insertButton.onclick = async event => {
      event.stopPropagation();
      await selectNode(node, element);
      await insertGeneratedIntoLeaf(node, insertButton);
    };
    if (!animate) return;
    const old = oldPositions.get(element.dataset.id);
    if (old) {
      const current = element.getBoundingClientRect();
      element.animate([
        {transform: `translate(${old.left - current.left}px,${old.top - current.top}px)`, opacity: .72},
        {transform: 'translate(0,0)', opacity: 1}
      ], {duration: 520, easing: 'cubic-bezier(.2,.8,.2,1)'});
    } else {
      element.classList.add('node-enter');
      element.addEventListener('animationend', () => element.classList.remove('node-enter'), {once: true});
    }
  });

  await new Promise(resolve => requestAnimationFrame(resolve));
  drawTreeEdges(nodes, scale);

  const replacement = closestSelectedNode(nodes);
  if (replacement) {
    const element = tree.querySelector(`[data-id="${CSS.escape(replacement.id)}"]`);
    await selectNode(replacement, element, false);
  }

  if (transition) animateMutation(transition, nodes);
}

function drawTreeEdges(nodes, scale) {
  const tree = $('tree');
  const svg = $('treeEdges');
  if (!svg) return;
  const treeRect = tree.getBoundingClientRect();
  const width = tree.scrollWidth;
  const height = tree.scrollHeight;
  svg.setAttribute('width', width);
  svg.setAttribute('height', height);
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  const paths = [];
  for (const child of nodes) {
    if (!child.parent_id) continue;
    const parentElement = tree.querySelector(`[data-id="${CSS.escape(child.parent_id)}"]`);
    const childElement = tree.querySelector(`[data-id="${CSS.escape(child.id)}"]`);
    if (!parentElement || !childElement) continue;
    const parent = parentElement.getBoundingClientRect();
    const target = childElement.getBoundingClientRect();
    const x1 = (parent.left + parent.width / 2 - treeRect.left) / scale;
    const y1 = (parent.bottom - treeRect.top) / scale;
    const x2 = (target.left + target.width / 2 - treeRect.left) / scale;
    const y2 = (target.top - treeRect.top) / scale;
    const bend = y1 + (y2 - y1) * .52;
    paths.push(`<path class="tree-edge" d="M ${x1} ${y1} C ${x1} ${bend}, ${x2} ${bend}, ${x2} ${y2}" marker-end="url(#treeArrow)"/>`);
  }
  svg.innerHTML = `<defs><marker id="treeArrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z"/></marker></defs>${paths.join('')}`;
}

async function selectNode(node, element, remember = true) {
  if (!node || !element) return;
  document.querySelectorAll('.node').forEach(candidate => candidate.classList.remove('selected'));
  element.classList.add('selected');
  if (remember) selectedNode = {...node};
  else selectedNode = {...node};
  $('nodeDetails').textContent = JSON.stringify(node, null, 2);
  const logical = node.node_kind === 'logical_range';
  const inspectable = logical ? node.bounded && !node.empty : node.is_leaf;
  $('leafItems').textContent = inspectable ? 'Loading…' : logical ?
    (node.empty ? 'Empty logical slot.' : 'Unbounded logical range: descend to a bounded range to inspect items.') :
    'Physical internal node: select a leaf to see key, route, and tuple hashes.';
  if (!inspectable) return;
  const requestId = ++leafRequest;
  try {
    const items = await api(`/api/leaf-items?partition=${node.partition_id}&prefix_len=${node.prefix_len}&prefix_hex=${node.prefix_hex}`);
    if (requestId !== leafRequest || selectedNode.id !== node.id) return;
    $('leafItems').innerHTML = `<div class="leaf-item-list">${items.map(item => `<div class="item" data-key="${item.key_text}">
      <div class="leaf-key-row"><span><b>key</b> ${item.key_text ?? item.key_data_hex}</span>
      <button class="leaf-delete" data-key="${item.key_text}" title="Delete key ${item.key_text}">Delete</button></div>
      <b>key bytes</b> ${item.key_data_hex}<br><b>route</b> ${item.route_digest}<br><b>tuple</b> ${item.tuple_hash}</div>`).join('') || '<div class="item">Empty leaf</div>'}</div>`;
    document.querySelectorAll('.leaf-delete').forEach(button => button.onclick = () => deleteLeafKey(button.dataset.key));
  } catch (error) {
    if (requestId === leafRequest) $('leafItems').textContent = error.message;
  }
}

async function insertGeneratedIntoLeaf(node, button) {
  const originalLabel = button.textContent;
  button.disabled = true;
  button.textContent = 'Finding key…';
  try {
    let candidate = null;
    let startKey = null;
    for (let search = 0; search < 16 && !candidate; search++) {
      const payload = {partition_id: node.partition_id, prefix_len: node.prefix_len,
        prefix_hex: node.prefix_hex, count: 1, max_attempts: 500000};
      if (startKey != null) payload.start_key = startKey;
      const result = await api('/api/leaf-key-candidates', payload);
      candidate = result.matches[0] || null;
      startKey = result.next_start_key;
    }
    if (!candidate) throw Error('No unused 32-bit key found after an exhaustive multi-batch native route search');
    button.textContent = 'Inserting…';
    const updated = await api('/api/mutate', {
      operation: 'insert', key: candidate.key, fields: candidate.fields,
      expected_leaf: {partition_id: node.partition_id, prefix_len: node.prefix_len, prefix_hex: node.prefix_hex}
    });
    const transition = updated.transition || {operation: 'insert', key: candidate.key};
    if (transition.selected_leaf_match !== true)
      throw Error(`Generated key ${candidate.key} did not remain in the selected native prefix`);
    timeline.push({statement: `generated leaf insert key=${candidate.key}`, ...transition});
    addEvents([]);
    await render(updated, transition);
    toast(`Inserted generated key ${candidate.key} into selected leaf; rows=${updated.row_count}`);
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = originalLabel;
  }
}

function normalizeTransition(input, before, after) {
  const beforeIds = new Set((before?.nodes || []).filter(node => node.is_leaf).map(node => node.id));
  const afterIds = new Set((after?.nodes || []).filter(node => node.is_leaf).map(node => node.id));
  const added = input.added_leaves || (after?.nodes || []).filter(node => node.is_leaf && !beforeIds.has(node.id));
  const removed = input.removed_leaves || (before?.nodes || []).filter(node => node.is_leaf && !afterIds.has(node.id));
  return {
    operation: input.operation || (input.statement || 'mutation').trim().split(/\s+/, 1)[0].toLowerCase(),
    key: input.key ?? null,
    split_delta: +(input.split_delta || 0),
    merge_delta: +(input.merge_delta || 0),
    added_leaves: added,
    removed_leaves: removed,
    statement: input.statement || ''
  };
}

function leafDescription(leaves) {
  if (!leaves.length) return 'none';
  return leaves.map(leaf => `P${leaf.partition_id} / ${leaf.prefix_bits || `prefix length ${leaf.prefix_len}`}`).join(', ');
}

function transitionSteps(transition, after) {
  const structural = transition.split_delta || transition.merge_delta || transition.added_leaves.length || transition.removed_leaves.length;
  const action = transition.operation.toUpperCase();
  return [
    {title: '1 · Route and commit mutation', detail: `${action}${transition.key == null ? '' : ` key ${transition.key}`} completed through PostgreSQL's native index mutation path.`},
    {title: '2 · Refresh affected leaf data', detail: `Tuple membership, tuple hash, data XOR, and row count now reflect the committed ${transition.operation}.`},
    {title: structural ? `3 · Apply native ${transition.split_delta ? 'split' : 'merge'} frontier` : '3 · Frontier remains stable',
     detail: structural ? `Removed: ${leafDescription(transition.removed_leaves)}. Added: ${leafDescription(transition.added_leaves)}.` : 'No native leaf prefixes were added or removed.'},
    {title: '4 · Recompute ancestors and roots', detail: `Combined, data, and structure roots were read from layout-v5 after the committed change.`},
    {title: '5 · Verify native tree', detail: after.verify ? 'merkle_dynamic_verify returned PASS.' : 'Native verification did not pass.'}
  ];
}

function drawTransitionSteps(steps, active = -1) {
  $('transitionStages').innerHTML = steps.map((step, index) =>
    `<div class="transition-stage ${index < active ? 'done' : index === active ? 'active' : ''}">
      <b>${step.title}</b><span>${step.detail}</span></div>`
  ).join('');
}

async function playTransition(transition, after, replay = false) {
  const steps = transitionSteps(transition, after);
  const structural = transition.split_delta || transition.merge_delta || transition.added_leaves.length || transition.removed_leaves.length;
  const kind = transition.split_delta ? `split +${transition.split_delta}` : transition.merge_delta ? `merge +${transition.merge_delta}` : transition.operation;
  $('transitionSummary').textContent = `${replay ? 'Replaying explanation of' : 'Observed'} ${kind}${transition.key == null ? '' : ` for key ${transition.key}`}`;
  drawTransitionSteps(steps);
  const slow = structural && $('slowStructural').checked;
  const delay = +$('stageDelay').value;

  if (!replay) {
    logicalTreeCache.clear();
    if (slow) {
      for (let stage = 0; stage < steps.length; stage++) {
        drawTransitionSteps(steps, stage);
        if (stage === 2) {
          snapshot = after;
          updateSnapshotChrome(after);
          await renderTree(true, transition);
        }
        await wait(delay);
      }
    } else {
      snapshot = after;
      updateSnapshotChrome(after);
      await renderTree(true, transition);
    }
  } else {
    for (let stage = 0; stage < steps.length; stage++) {
      drawTransitionSteps(steps, stage);
      animateMutation(transition, snapshot?.nodes || []);
      await wait(delay);
    }
  }
  drawTransitionSteps(steps, steps.length);
}

function animateMutation(transition, nodes) {
  const operation = ['insert', 'update', 'delete'].includes(transition.operation) ? transition.operation : 'update';
  let targets = [];
  const addedIds = new Set(transition.added_leaves.map(leafId));
  if (addedIds.size) targets = [...document.querySelectorAll('.node')].filter(node => addedIds.has(node.dataset.id));
  if (!targets.length) targets = [...document.querySelectorAll('.node.selected')];
  if (!targets.length && transition.added_leaves.length) {
    const partition = transition.added_leaves[0].partition_id;
    targets = [...document.querySelectorAll('.node')].filter(node => nodes.find(value => value.id === node.dataset.id)?.partition_id === partition);
  }
  targets.forEach(target => {
    target.classList.remove('mutation-insert', 'mutation-update', 'mutation-delete');
    void target.offsetWidth;
    target.classList.add(`mutation-${operation}`);
  });
  $('metrics').classList.remove('metrics-pulse');
  void $('metrics').offsetWidth;
  $('metrics').classList.add('metrics-pulse');
}

async function deleteLeafKey(key) {
  if (key == null || key === '') return;
  try {
    const updated = await api('/api/mutate', {operation: 'delete', key: +key});
    const transition = updated.transition || {operation: 'delete', key: +key};
    timeline.push({statement: `leaf delete key=${key}`, ...transition});
    addEvents([]);
    await render(updated, transition);
    toast(`Deleted key ${key}; rows=${updated.row_count}`);
  } catch (error) {
    toast(error.message, true);
  }
}

function showNext(next) {
  $('nextStep').textContent = next ? `Next: ${next.kind} · key ${next.key ?? 'unresolved'} · partition ${next.partition ?? 'not resolved'}` : 'Next step: workload complete';
}

function addEvents(events) {
  events.forEach(event => timeline.push(event));
  timeline = timeline.slice(-80);
  $('timeline').innerHTML = timeline.map(event => `<div class="event ${event.error ? 'merge' : event.split_delta ? 'split' : event.merge_delta ? 'merge' : ''}">
    <b>${event.error ? 'Error (cursor advanced)' : event.split_delta ? `Split +${event.split_delta}` : event.merge_delta ? `Merge +${event.merge_delta}` : 'Mutation'}</b><br>
    ${(event.statement || '').slice(0, 120)}${event.error ? `<br>${event.error}` : ''}</div>`).join('');
}

$('build').onclick = async () => {
  try {
    const [file, text] = await fileText('dataset');
    selectedNode = null;
    await render(await api('/api/build', {source: 'upload', format: file.name.endsWith('.jsonl') ? 'jsonl' : 'csv', content: text, config: config()}));
    timeline = [];
    toast('Native dynamic Merkle index built and verified');
  } catch (error) { toast(error.message, true); }
};

$('canonical').onclick = async () => {
  try {
    selectedNode = null;
    await render(await api('/api/build', {source: 'canonical_restore', config: config()}));
    timeline = [];
    toast('Loaded canonical 11,994-row base and built native index');
  } catch (error) { toast(error.message, true); }
};

$('clone').onclick = async () => {
  try {
    selectedNode = null;
    await render(await api('/api/build', {source: 'existing_usertable_small', config: config()}));
    timeline = [];
    toast('Cloned current usertable_small and built native index');
  } catch (error) { toast(error.message, true); }
};

$('loadWorkload').onclick = async () => {
  try {
    const [, text] = await fileText('workload');
    const result = await api('/api/workload/load', {content: text});
    $('progress').max = result.loaded;
    $('progress').value = 0;
    $('workloadStatus').textContent = `0 / ${result.loaded}`;
    showNext(result.next);
    if (result.compatible) toast(`Loaded ${result.loaded} compatible statements`);
    else toast(`${result.conflicting_insert_count} workload insert keys already exist. Use canonical 12k base for the 20/0 run.`, true);
  } catch (error) { toast(error.message, true); }
};

async function runWorkload(path) {
  try {
    const result = await api(path, {});
    addEvents(result.events);
    const event = result.events.length === 1 ? result.events[0] :
      result.events.find(candidate => candidate.split_delta || candidate.merge_delta) || result.events[result.events.length - 1];
    await render(result.snapshot, event && !event.error ? event : null);
    showNext(result.next);
    $('progress').max = result.total;
    $('progress').value = result.cursor;
    $('workloadStatus').textContent = `${result.cursor} / ${result.total}${result.done ? ' · complete' : ''}`;
  } catch (error) { toast(error.message, true); }
}

$('step').onclick = () => runWorkload('/api/workload/step');
$('run').onclick = () => runWorkload('/api/workload/run');

$('mutate').onclick = async () => {
  try {
    const operation = $('operation').value;
    const fields = operation === 'delete' ? {} : JSON.parse($('fields').value);
    const updated = await api('/api/mutate', {operation, key: +$('key').value, fields});
    const transition = updated.transition || {operation, key: +$('key').value};
    timeline.push({statement: `manual ${operation} key=${$('key').value}`, ...transition});
    addEvents([]);
    await render(updated, transition);
    toast(`${operation} applied`);
  } catch (error) { toast(error.message, true); }
};

$('refresh').onclick = async () => {
  try { await render(await api('/api/snapshot')); } catch (error) { toast(error.message, true); }
};

$('resetCounters').onclick = async () => {
  try {
    await render(await api('/api/reset-counters', {}));
    timeline = [];
    $('timeline').innerHTML = '';
    toast('Split and merge counters reset');
  } catch (error) { toast(error.message, true); }
};

$('replayTransition').onclick = async () => {
  if (lastPlayback) await playTransition(lastPlayback.transition, lastPlayback.after, true);
};
$('partition').onchange = () => renderTree(false);
$('hashMode').onchange = () => renderTree(false);
$('zoom').oninput = () => renderTree(false);
$('treeView').onchange = async () => {
  selectedNode = null;
  $('nodeDetails').textContent = 'Click a physical node or logical range to inspect native metadata.';
  $('leafItems').textContent = 'Select a physical leaf or bounded logical range.';
  try { await renderTree(false); } catch (error) { toast(error.message, true); }
};
$('showEmptyLogical').onchange = async () => {
  selectedNode = null;
  try { await renderTree(false); } catch (error) { toast(error.message, true); }
};

if (initialView.get('view') === 'logical') $('treeView').value = 'logical';
if (['1', 'true', 'yes', 'on'].includes((initialView.get('empty') || '').toLowerCase()))
  $('showEmptyLogical').checked = true;

api('/api/snapshot').then(value => render(value)).catch(error => toast(error.message, true));
