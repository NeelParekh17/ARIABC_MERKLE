#!/usr/bin/env python3
"""Render source-grounded Merkle figures in the LSM architecture guide's style.

Run on the lab host: python3 render.py --out <isolated-output-directory>
Pillow and DejaVu Sans are the only rendering dependencies.
"""
import argparse
import json
import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

COLORS = {
    'blue': ('#e7f0fa', '#7395b5'),
    'green': ('#e2f3eb', '#448f76'),
    'amber': ('#fff2d7', '#bd8726'),
    'gray': ('#eff3f6', '#7395b5'),
    'red': ('#fce8e6', '#bb6d69'),
}
INK = '#193049'
MUTED = '#5c728b'
FONT = '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'
BOLD = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'
SCALE = 1.4
MANIFEST = []
ERRORS = []


class Figure:
    def __init__(self, name, title, subtitle='', height=1000, footer=''):
        self.name, self.title, self.subtitle = name, title, subtitle
        self.w, self.h, self.footer = 1600, height, footer
        self.nodes, self.edges, self.labels = {}, [], []

    def box(self, key, text, x, y, w=420, h=140, color='blue'):
        self.nodes[key] = (x, y, w, h, text, color)
        return key

    def edge(self, start, end, points=None, color='blue', dashed=False):
        self.edges.append((start, end, points, color, dashed))

    def branch(self, parent, children, junction_y, collect=False, dashed=False):
        """One shared orthogonal branch; arrowheads stay clear of junctions."""
        x, y, w, h, *_ = self.nodes[parent]
        px, py = x + w / 2, y if collect else y + h
        for child in children:
            cx, cy, cw, ch, *_ = self.nodes[child]
            cx += cw / 2
            cy += ch if collect else 0
            points = [(px, py), (px, junction_y), (cx, junction_y), (cx, cy)]
            if collect:
                self.edge(child, parent, list(reversed(points)), dashed=dashed)
            else:
                self.edge(parent, child, points, dashed=dashed)

    def label(self, text, x, y, size=25, bold=False):
        self.labels.append((text, x, y, size, bold))

    def wrap(self, text, font, max_width, draw):
        result = []
        for paragraph in text.split('\n'):
            words = paragraph.split()
            if not words:
                result.append('')
                continue
            line = words.pop(0)
            for word in words:
                candidate = line + ' ' + word
                if draw.textlength(candidate, font=font) <= max_width:
                    line = candidate
                else:
                    result.append(line)
                    line = word
            result.append(line)
        return result

    def render(self, output):
        image = Image.new('RGB', (round(self.w*SCALE), round(self.h*SCALE)), 'white')
        d = ImageDraw.Draw(image)
        font = lambda size, bold=False: ImageFont.truetype(BOLD if bold else FONT, round(size*SCALE))
        pos = lambda p: tuple(round(v*SCALE) for v in p)
        def text_block(text, x, y, size=25, bold=False, width=None, center=None, color=INK):
            f = font(size, bold)
            wrapped = self.wrap(text, f, (width or 1450)*SCALE, d)
            line_h = round(size*1.34*SCALE)
            total_h = len(wrapped)*line_h
            if center:
                x, y, w, h = center
                if total_h > (h-28)*SCALE:
                    ERRORS.append(f'{self.name}: text too tall: {text}')
                yy = round((y+h/2)*SCALE-total_h/2)
                for line in wrapped:
                    tw = d.textlength(line, font=f)
                    if tw > (w-28)*SCALE:
                        ERRORS.append(f'{self.name}: text too wide: {line}')
                    d.text((round((x+w/2)*SCALE-tw/2), yy), line, font=f, fill=color)
                    yy += line_h
            else:
                yy = round(y*SCALE)
                for line in wrapped:
                    d.text((round(x*SCALE), yy), line, font=f, fill=color)
                    yy += line_h
            return total_h/SCALE

        title_height=text_block(self.title, 54, 42, 39, True)
        if title_height > 56:
            ERRORS.append(f'{self.name}: title wraps into subtitle')
        text_block(self.subtitle, 56, 108, 21, color=MUTED)
        for start, end, points, color, dashed in self.edges:
            a, b = self.nodes[start], self.nodes[end]
            if points is None:
                ax, ay = a[0]+a[2]/2, a[1]+a[3]/2
                bx, by = b[0]+b[2]/2, b[1]+b[3]/2
                if b[1] >= a[1]+a[3]+30 or a[1] >= b[1]+b[3]+30:
                    points = [(ax, a[1]+a[3] if by>ay else a[1]),
                              (bx, b[1] if by>ay else b[1]+b[3])]
                else:
                    points = [(a[0]+a[2] if bx>ax else a[0], ay),
                              (b[0] if bx>ax else b[0]+b[2], by)]
            # Do not hide incorrect connectors underneath unrelated boxes.
            for p0,p1 in zip(points,points[1:]):
                for key,(x,y,w,h,*_) in self.nodes.items():
                    if key in (start,end):
                        continue
                    for sample in range(1,100):
                        t=sample/100
                        xx=p0[0]+(p1[0]-p0[0])*t
                        yy=p0[1]+(p1[1]-p0[1])*t
                        if x+3 < xx < x+w-3 and y+3 < yy < y+h-3:
                            ERRORS.append(f'{self.name}: connector {start}→{end} crosses box {key}')
                            break
            pp = [pos(p) for p in points]
            c = COLORS[color][1]
            for a0, b0 in zip(pp, pp[1:]):
                if dashed:
                    dx, dy = b0[0]-a0[0], b0[1]-a0[1]
                    length = math.hypot(dx,dy)
                    for offset in range(0, int(length), 18):
                        t0, t1 = offset/max(1,length), min(1,(offset+10)/max(1,length))
                        d.line([(a0[0]+dx*t0,a0[1]+dy*t0),(a0[0]+dx*t1,a0[1]+dy*t1)],fill=c,width=4)
                else:
                    d.line([a0,b0],fill=c,width=4,joint='curve')
            a0,b0=pp[-2:]
            angle=math.atan2(b0[1]-a0[1], b0[0]-a0[0])
            tip=16*SCALE
            wing=7*SCALE
            d.polygon([b0,(b0[0]-tip*math.cos(angle)+wing*math.sin(angle),b0[1]-tip*math.sin(angle)-wing*math.cos(angle)),
                          (b0[0]-tip*math.cos(angle)-wing*math.sin(angle),b0[1]-tip*math.sin(angle)+wing*math.cos(angle))],fill=c)
        for key,(x,y,w,h,text,color) in self.nodes.items():
            if x<40 or y<165 or x+w>1560 or y+h>self.h-105:
                ERRORS.append(f'{self.name}: box outside content bounds: {key}')
            fill, outline=COLORS[color]
            d.rounded_rectangle(pos((x,y,x+w,y+h)),radius=14*SCALE,fill=fill,outline=outline,width=3)
            text_block(text,x,y,24,width=w-38,center=(x,y,w,h))
        for text,x,y,size,bold in self.labels:
            text_block(text,x,y,size,bold)
        text_block(self.footer,58,self.h-78,20,width=1480,color=MUTED)
        image.save(output/(self.name+'.png'),optimize=True)
        MANIFEST.append({'name':self.name,'title':self.title,'width':image.width,'height':image.height})


def pipeline(name,title,steps,subtitle='',footer=''):
    """A spaced, numbered process: 3 cards per row, with routed continuation."""
    rows=math.ceil(len(steps)/3)
    f=Figure(name,title,subtitle,height=290+rows*240,footer=footer)
    for i,step in enumerate(steps):
        label,color=step if isinstance(step,tuple) else (step,'blue')
        row,col=divmod(i,3)
        f.box(str(i),f'{i+1}. {label}',70+col*510,205+row*240,440,160,color)
        if i:
            if col:
                f.edge(str(i-1),str(i))
            else:
                previous_y=205+(row-1)*240
                f.edge(str(i-1),str(i),[(1530,previous_y+80),(1550,previous_y+80),
                    (1550,previous_y+205),(50,previous_y+205),(50,205+row*240+80),(70,205+row*240+80)])
    return f


def lanes(name,title,rows,subtitle='',footer=''):
    """Question / state on the left, its precise result on the right."""
    f=Figure(name,title,subtitle,height=290+len(rows)*215,footer=footer)
    for i,(left,right,*colors) in enumerate(rows):
        y=205+i*215
        f.box(f'l{i}',left,70,y,600,145,colors[0] if colors else 'gray')
        f.box(f'r{i}',right,900,y,630,145,colors[1] if len(colors)>1 else 'green')
        f.edge(f'l{i}',f'r{i}')
    return f


def snake_pipeline(name,title,steps,subtitle='',footer=''):
    """Two columns with short alternating arrows and vertical row changes."""
    rows=math.ceil(len(steps)/2)
    f=Figure(name,title,subtitle,height=340+rows*260,footer=footer)
    for i,step in enumerate(steps):
        label,color=step if isinstance(step,tuple) else (step,'blue')
        row,col=divmod(i,2)
        if row%2:
            col=1-col
        f.box(str(i),f'{i+1}. {label}',70+col*830,205+row*260,630,170,color)
        if i:
            f.edge(str(i-1),str(i))
    return f


def tree(name,title,root,children,subtitle='',footer='',grandchildren=None):
    n=len(children)
    height=1080 if grandchildren else 820
    f=Figure(name,title,subtitle,height,footer)
    f.box('root',root,535,205,530,145,'green')
    cw=(1460-45*(n-1))/n
    for i,c in enumerate(children):
        text,color=c if isinstance(c,tuple) else (c,'green')
        x=70+i*(cw+45)
        f.box(f'c{i}',text,x,495,cw,155,color)
    f.branch('root',[f'c{i}' for i in range(n)],420)
    if grandchildren:
        parent, labels=grandchildren
        gw=(1460-45*(len(labels)-1))/len(labels)
        for i,label in enumerate(labels):
            f.box(f'g{i}',label,70+i*(gw+45),795,gw,155,'green')
        f.branch(f'c{parent}',[f'g{i}' for i in range(len(labels))],720)
    return f


def panels(name,title,headers,rows,subtitle='',footer=''):
    """Two before/after columns, matching the reference document."""
    f=Figure(name,title,subtitle,360+len(rows)*215,footer)
    f.label(headers[0],80,184,27,True)
    f.label(headers[1],870,184,27,True)
    for i,(left,right) in enumerate(rows):
        y=245+i*215
        f.box(f'a{i}',left,80,y,650,145,'gray')
        f.box(f'b{i}',right,870,y,650,145,'green')
        f.edge(f'a{i}',f'b{i}')
    return f


def figures():
    out=[]
    add=out.append
    add(pipeline('m01_overview','From SQL mutation to a committed Merkle tree',[
        ('INSERT / UPDATE / DELETE\nchange application rows','gray'),
        'Executor / AM callback\nor BCDB physical apply',
        ('Key → route\nFull row → contribution','green'),
        ('Stage signed hash + count deltas\nin transaction memory','amber'),
        'Before commit: combine by leaf\nthen combine by ancestor',
        ('Update nodes; check split / merge\ncommit with application changes','green')],
        'The diagram shows responsibilities; these steps are not parallel callbacks.'))
    f=tree('m02_forest','One application table owns many partition roots',
        'Read-time table digest\nSUM of partition-root hashes',
        ['Partition 0\nindependent tree','Partition 1\nindependent tree','Partition P−1\nindependent tree'],
        'There is no additional stored global-root node.',
        'A bucket leaf summarizes multiple rows; these groups are not PostgreSQL table partitions.')
    f.h=1420
    f.label('ZOOM INTO PARTITION 0',70,725,27,True)
    f.box('zoom','Partition 0 root',535,805,530,120,'green')
    for i,label in enumerate(['Bucket leaf','Internal node\nfurther children','Bucket leaf','Bucket leaf']):
        f.box(f'g{i}',label,70+i*380,1100,320,150,'green')
    f.branch('zoom',[f'g{i}' for i in range(4)],1010)
    add(f)
    add(lanes('m03_storage','Physical storage: application data and tree metadata',[
        ('Application-table heap\ncomplete user rows','Application PK B-tree\nMerkle row-lookup B-tree','gray','blue'),
        ('Custom USING merkle relation\nblock 0: configuration metapage','Dedicated internal node heap\none PostgreSQL tuple per Merkle node','blue','green'),
        ('Node-table access paths','Primary-key B-tree\nPrefix B-tree\nPartial root B-tree','green','amber')],
        'Example node table: ariabc_internal.merkle_node_usertable',
        'Logical Merkle edges are computed from prefixes; B-tree entries locate physical heap tuples.'))
    add(pipeline('m04_prefix_walk','Follow the leading bits until a bucket leaf',[
        ('Route starts\n10 01 11 ...','gray'),
        'Root: prefix length 0\nconsume next 2 bits: 10',
        'Node 10: prefix length 2\nconsume next 2 bits: 01',
        ('Node 1001: prefix length 4\nis_leaf = true','green'),
        ('Stop at this bucket\nremaining bits are unused','amber')],
        'Illustrative fanout 4: each level consumes two route bits.'))
    add(lanes('m05_settings','Four controls determine the tree geometry',[
        ('Fanout F','Children per split\nbits per level = log₂ F'),
        ('Split threshold S','Split after growth when leaf N > S'),
        ('Merge threshold M','After deletion: all sibling rows\ntogether must be at most M'),
        ('Partition count P','P independent logical roots')],
        'Bare backend defaults: F=4, S=32, M=8, P=200. Benchmark scripts can override them.'))
    add(lanes('m06_three_lookups','Three questions use three different indexes',[
        ('Find user row\nycsb_key = K','Application primary-key B-tree\n→ application heap row','gray','blue'),
        ('Find tree node\npartition + node ID + prefix length','Node-table primary-key B-tree\n→ internal node heap tuple','gray','green'),
        ('Find all user rows in a bucket\npartition + route interval','Application Merkle lookup B-tree\n→ matching application heap rows','gray','amber')]))
    add(pipeline('m07_range_lookup','A leaf prefix becomes a user-row range query',[
        ('Leaf identity\npartition + padded prefix','green'),
        'Compute inclusive\nlower / upper route bytes',
        ('Lookup B-tree seek\npartition equality + route range','amber'),
        'Read keys and heap TIDs',
        ('Fetch complete user rows\nwhen payload is required','gray'),
        ('Hash rows for split\nor transfer rows for repair','green')],
        'The lookup index covers routing expressions and keys, not every payload column.'))
    f=Figure('m08_two_hashes','One row, two hashes, two different jobs',
        'Example row: key=42, balance=100, name=Ana',1050,
        'Changing balance keeps the key route but changes the full-row integrity contribution.')
    f.box('row','Complete logical row',535,195,530,110,'gray')
    f.box('key','Indexed key columns only\ncanonical key encoding',80,420,650,140,'blue')
    f.box('full','All live user columns\ncanonical row encoding',870,420,650,140,'green')
    f.box('route','BLAKE3-256 → first 8 bytes\n64-bit route\npartition + prefix path',80,710,650,180,'blue')
    f.box('hash','BLAKE3-256 → all 32 bytes\nh(row)\nadditive integrity contribution',870,710,650,180,'green')
    for a,b in [('row','key'),('row','full'),('key','route'),('full','hash')]:f.edge(a,b)
    add(f)
    add(snake_pipeline('m09_leading_route','Leading-key groups choose the partition',[
        'Hash leading key columns\nlead = first 64 bits',
        'group = lead mod (P / s)',
        'Hash full key\noffset = full route mod s',
        ('target = group × s + offset','amber'),
        'Adjust route bytes so\nroute value mod P = target',
        ('Use adjusted bytes for\npartition and prefix traversal','green')],
        'Enabled by partition_key_columns > 0; s is the subpartitions option.'))
    add(tree('m10_sum_tree','Node hashes add row contributions',
        'Root\nH = hA + hB + hC; N = 3',
        ['Leaf 00\nH = hA + hB\nN = 2','Leaf 01\nH = hC\nN = 1','Leaf 10\nH = 0\nN = 0','Leaf 11\nH = 0\nN = 0'],
        'All additions are modulo 2²⁵⁶; example topology uses a small explicit threshold.',
        'Parents add child hashes; they do not calculate BLAKE3(left || right).'))
    add(pipeline('m11_insert_callback','INSERT: stage the new row contribution',[
        ('Executor inserts a new heap tuple','gray'),
        'aminsert calls merkleInsert\nwith keys and new heap TID',
        'Compute key route\nfetch complete new heap row',
        ('Compute canonical\n32-byte row hash','green'),
        ('Stage INSERT\nΔH = +h(new); ΔN = +1','amber'),
        'Before commit: apply nodes\nand consider split thresholds'],
        'The Merkle AM does not enforce uniqueness.'))
    add(pipeline('m12_insert_path','INSERT: the same delta reaches every ancestor',[
        ('Inserted row\n+h, +1','gray'),
        ('Target leaf\nH += h; N += 1','green'),
        ('Parent\nH += h; N += 1','green'),
        ('Partition root\nH += h; N += 1','green')],
        'Shown for one event; commit-time coalescing combines events that share nodes.',
        'Unrelated leaves and partitions receive no contribution change.'))
    add(pipeline('m13_delete_capture','DELETE: capture first, subtract only after success',[
        ('Fetch old visible row','gray'),
        'CaptureMerkleDeletePlan\nold route + positive h(old)',
        'table_tuple_delete',
        'Only TM_Ok stages the\ncaptured DELETE plan',
        ('Staging negates old hash\nΔH = −h(old); ΔN = −1','amber'),
        ('Commit-time apply updates\nleaf and ancestors','green')],
        'Retries, skipped deletes and errors follow the executor path.',
        'VACUUM callbacks report statistics; they do not subtract the row again.'))
    add(pipeline('m14_update_sql','Ordinary UPDATE: maintain Merkle exactly once',[
        ('Capture old row route + hash','gray'),
        'table_tuple_update\nrequire successful TM_Ok',
        ('Stage old removal\n−h(old), −1','amber'),
        'For non-HOT index insertion:\nsuppress generic Merkle callback',
        ('Explicitly add new row once\n+h(new), +1; also for HOT','green'),
        'Commit applier combines\nold and new events'],
        'HOT skips ordinary index insertion, but the full-row integrity hash still changes.'))
    add(tree('m15_same_leaf','A same-leaf UPDATE keeps the count unchanged',
        'Final leaf delta\nΔH = h(new) − h(old); ΔN = 0',
        [('Old row removal\n−h(old), −1','red'),('New row addition\n+h(new), +1','green')],
        'The signed contributions combine before physical node updates.',
        'Every ancestor receives the same net hash change and zero net count change.'))
    add(lanes('m16_bcdb_shortcut','BCDB UPDATE chooses one event or two',[
        ('Optimistic execution','Record write-set intent\nphysical heap changes happen later'),
        ('Physical apply','Read old/new heap images\ncompute both hashes and routes'),
        ('Old 8 route bytes = new 8 route bytes','One UPDATE_SAME_LEAF event\nh(new) − h(old), count delta 0','blue','green'),
        ('Route bytes differ','DELETE old route + INSERT new route\nresolve actual leaves at commit','blue','amber')],
        'Different route bytes may still land in the same current bucket.'))
    add(lanes('m17_apply_timing','When the staged Merkle changes are applied',[
        ('Ordinary SQL transaction','PRE_COMMIT applies staged deltas'),
        ('BCDB worker after physical-apply\nretry helper returns','If synchronous-direct is enabled:\napply staged deltas before PRE_COMMIT'),
        ('PRE_COMMIT after an earlier apply','Already-applied flag skips\nduplicate application'),
        ('Attempt to stage further nonzero deltas','Rejected after durable delta finalization\nno second batch of leftover deltas','gray','red'),
        ('PostgreSQL transaction commit','Application heap + node changes\ncommit together','gray','green')],
        'Both paths call the same transaction-local synchronous apply routine.'))
    add(tree('m18_move_same_partition','A key move can change two branches',
        'Shared partition root\nΔH = h(new) − h(old); ΔN = 0',
        [('Old branch → old leaf\n−h(old), −1','red'),('New branch → new leaf\n+h(new), +1','green')],
        'Illustrative update between two leaves in the same partition.',
        'Each exclusive branch gets its signed delta; common ancestors get their combined delta.'))
    add(tree('m19_move_partition','A key move can change two partition roots',
        'Read-time table digest change\nh(new) − h(old)',
        [('Partition 3 root\n−h(old), −1','red'),('Partition 9 root\n+h(new), +1','green')],
        'Partition roots are stored independently; the table digest is computed when read.',
        'Total row count stays unchanged. Old/new paths may become merge/split eligible.'))
    add(lanes('m20_stage_combine','Repeated staging events combine in memory',[
        ('Same staging key: index, physical identity,\nevent type, old/new route bytes','Add signed hash deltas\nand count deltas','gray','amber'),
        ('Both resulting deltas are zero','Remove the map entry'),
        ('Either resulting delta is nonzero','Retain the combined delta entry')],
        'This first map is keyed by events/routes, not by the final physical leaf.'))
    add(lanes('m21_savepoints','SAVEPOINT frames follow transaction outcomes',[
        ('Release / commit subtransaction','Merge child delta entries\ninto the parent frame'),
        ('Roll back subtransaction','Discard the child delta frame','gray','red'),
        ('Outer transaction commits','Apply surviving deltas\ncommit heap + nodes','gray','green'),
        ('Outer transaction aborts','PostgreSQL rolls back mutations\ndiscard transaction staging','gray','red')],
        'Prepared transactions with staged Merkle changes are rejected.'))
    add(pipeline('m22_commit_coalesce','Commit-time apply combines work twice',[
        'Combine surviving frames\nsort by index / partition / route',
        'Resolve each event\nto its current leaf',
        ('Combine deltas by leaf\nupdate in canonical order','green'),
        'Re-route stale pending events\nafter concurrent shape changes',
        ('Combine deltas by ancestor\napply nonzero node changes','green'),
        ('Re-read queued leaf counts\ncheck split / merge thresholds','amber')],
        'Node order: partition ascending, prefix length descending, node ID ascending.'))
    add(tree('m23_shared_ancestor','Many events can produce one root update',
        'Shared root\ncombined Δ1 + Δ2 + Δ3',
        ['Leaf A\ncombine Δ1 + Δ2','Leaf B\napply Δ3'],
        'The ancestor phase accumulates all contributions before updating the root tuple.',
        'Zero deltas can cancel; retries and topology changes can add work.'))
    add(lanes('m24_leaf_resolution','Resolve a leaf: cache first, then indexed traversal',[
        ('Prefix-route cache hit','Use candidate leaf prefix\nvalidate at update time'),
        ('Cache miss','Start at zero root\nnode PK B-tree lookup at each level'),
        ('Encounter internal node','Consume next log₂ F route bits\nextend prefix and continue'),
        ('Leaf changed or disappeared','Invalidate cache\nre-route pending events','gray','amber')],
        'Cache entries include index physical identity and partition; SPI plans are the fallback.'))
    add(lanes('m25_physical_node_update','Update one physical node tuple safely',[
        ('Lookup a node','Validated route TID hint\nor node primary-key B-tree lookup'),
        ('Read current hash/count','Calculate replacement tuple\ncall table_tuple_update'),
        ('Result: TM_Updated','Follow successor; recompute from newer values\nretry the update'),
        ('Result: TM_Ok, HOT','Done; no new node B-tree entries','blue','green'),
        ('Result: TM_Ok, non-HOT','Maintain node-table index entries','blue','amber')],
        'Count zero clears the hash; negative calculated counts are clamped. See the separate count-rules figure.'))
    add(pipeline('m26_concurrent_shape','A cached leaf can become stale',[
        'Transaction A resolves\na candidate leaf',
        ('Transaction B splits / merges\nthe relevant subtree','amber'),
        'A tries to update the\nold candidate leaf',
        ('Node removed / leaf flag changed\nreturn stale-route result','red'),
        'Invalidate the cached route',
        ('Resolve the current leaf\nand retry pending delta','green')],
        'This is the retry mechanism, not a proof of every possible concurrent schedule.'))
    add(snake_pipeline('m27_split_process','Split an oversized bucket into child buckets',[
        'Positive count delta\nN > S and current prefix < 60',
        'Advisory lock\nrecheck that node is a leaf',
        ('Range-query actual user rows\npartition + route interval','amber'),
        'Bucket row contributions\nby next log₂ F route bits',
        ('Create all F children\nrecursively split oversized subsets','green'),
        ('Mark original node internal\nreplace totals from scanned rows','green')],
        'A nonempty successful split rebuilds local sums from the heap; ancestors are not reconciled.',
        'It is pure redistribution only if the stored node was consistent. Split also invalidates cached routes.'))
    add(lanes('m28_merge_process','Merge only when the whole sibling set qualifies',[
        ('A deletion leaves a non-root bucket at N ≤ M','Check all direct siblings:\nall leaves and combined population ≤ M'),
        ('Eligible sibling set','Lock with FOR UPDATE SKIP LOCKED\nrecheck latest counts and leaf flags','blue','amber'),
        ('Any sibling unavailable / latest check fails','Skip this optional merge','gray','gray'),
        ('Every sibling locked and still eligible','Sum hashes/counts; delete child rows\nparent becomes a leaf','green','green'),
        ('After a successful merge','Invalidate routes\nconsider merging the parent upward')],
        'One empty leaf alone is insufficient. A root never merges into another partition.'))
    add(pipeline('m29_bulk_build','CREATE INDEX / REINDEX builds nodes in bulk',[
        'Validate definition\ninitialize WAL-logged metapage',
        ('Scan visible application rows\ncompute route + row hash','gray'),
        'Sort in-memory entries\nby partition and route',
        'Transactionally replace\ndedicated node storage',
        ('For each partition: root leaf\nor recursive child buckets','green'),
        ('Batch-insert node tuples\nmaintain their B-tree indexes','amber')],
        'Non-concurrent builds only; initial build scans the heap rather than the lookup B-tree.',
        'A non-READY recovery-state guard exists, but the current status helper always returns READY.'))
    add(lanes('m30_verify','What merkle_verify_index actually checks',[
        ('Stored partition roots','SUM root hashes\nSUM root counts','green','green'),
        ('Independently scanned heap rows','Recompute full-row hash sum\ncount visible rows','gray','blue'),
        ('Compare root totals with heap totals','Both hash and count must match'),
        ('Each stored internal node','Hash and count must equal\nthe sums of its direct children')],
        'Format/freshness checks run first; a failed comparison returns false.',
        'It does not independently route every heap row and reconstruct every bucket membership.'))
    add(pipeline('m31_replica_repair','Replica repair: roots → leaf ranges → actual rows',[
        'Compare reference and local\npartition-root hashes',
        'Read leaf sets inside\nmismatching partitions',
        'Compare leaf identities/hashes\nform differing route intervals',
        ('Fetch reference rows by range\nCOPY to temporary candidate table','amber'),
        'Index candidates by application PK\ndelete extras; upsert differences',
        ('Ordinary DML maintains Merkle\nverify; fall back to full copy if needed','green')],
        'Selective path only: one Merkle key column, an existing node table, and a real application primary key.',
        'Ineligible tables use checksums and copy only on mismatch. See the separate fallback decision figure.'))
    add(lanes('m32_source_map','Where each responsibility lives in the source',[
        ('Formats, options, row/key encoding','merkle.h\nmerkle.c\nmerkleutil.c'),
        ('Mutation capture and staging','merkleinsert.c / nodeModifyTable.c\nshm_transaction.c / merkledelta.c'),
        ('Route, apply, split, merge, bulk build','merkleapply.c\nmerklebuild.c'),
        ('Verification and replica repair','merkleverify.c\nariabc_pg/src/replica_repair.cxx')],
        'Exact paths and source anchors remain in the guide.'))

    # Text-layout replacements: native figures rather than screenshots of ASCII.
    add(lanes('t01_metapage','Custom index block 0 stores configuration',[
        ('Format and relation identity','version 11; heapRelid\nroute version 4; row-hash version 1'),
        ('Geometry','fanout / split threshold / merge threshold\nnumber of partitions'),
        ('Routing and retained baseline field','partition_key_columns / subpartitions\nbaselineApplySeq'),
        ('Actual tree nodes','Separate internal PostgreSQL heap\nnot additional custom Merkle pages','blue','green')]))
    add(lanes('t02_node_names','Application tables determine node-table names',[
        ('public.usertable','ariabc_internal.\nmerkle_node_usertable'),
        ('sales.orders','ariabc_internal.\nmerkle_node_sales_orders'),
        ('Unresolved application relation','Fallback:\nmerkle_node_<index_oid>')],
        'Only one Merkle index is permitted per application table.'))
    add(lanes('t03_node_record','One node is a six-column PostgreSQL tuple',[
        ('Population and ownership','tuple_count: int4\npartition_id: int2','gray','green'),
        ('Position and kind','prefix_len: int2, measured in bits\nis_leaf: boolean','gray','blue'),
        ('Prefix and integrity state','node_id: bytea, 8 payload bytes\nhash: bytea, 32 payload bytes','gray','amber')],
        'Logical primary key: (partition_id, node_id, prefix_len)',
        'Count means application rows in the subtree, not number of child nodes.'))
    add(tree('t04_prefix_tree','Node IDs are padded route prefixes',
        'Partition 7 root\nnode ID = 0000000000000000; len = 0',
        ['00; len 2\n00...','01; len 2\n40...','10; len 2\n80...','11; len 2\nC0...'],
        'Fanout 4. Hex bytes shown under each binary prefix.',
        'Root and child 00 share zero ID bytes; prefix length distinguishes their identities.',
        (2,['1000; len 4\n80...','1001; len 4\n90...','1010; len 4\nA0...','1011; len 4\nB0...'])))
    add(lanes('t05_prefix_bounds','A prefix describes an inclusive route interval',[
        ('Significant prefix: 1001\nprefix length = 4 bits','Node ID / lower bound\n9000000000000000'),
        ('Fill all remaining bits with ones','Inclusive upper bound\n9FFFFFFFFFFFFFFF','gray','amber'),
        ('Drop one fanout-4 level','Parent prefix 10, length 2\n8000000000000000')],
        'Membership requires the same partition and lower ≤ route bytes ≤ upper.'))
    add(pipeline('t06_btree_point','A node B-tree lookup ends at a node heap tuple',[
        'Requested node key\npartition 7, ID 90..., length 4',
        'B-tree root page',
        'B-tree internal page',
        ('B-tree leaf entry\nexact key → heap TID','amber'),
        ('Internal node-table heap tuple\ncount, partition, length, leaf?, ID, hash','green')],
        'The physical B-tree hierarchy and logical Merkle hierarchy are different structures.'))
    add(lanes('t07_node_indexes','The three node-table B-trees use different ordering',[
        ('PRIMARY KEY B-tree','partition → node ID → prefix length\nthree equalities: exact node'),
        ('PREFIX B-tree','partition → prefix length → node ID\nequality, equality, range: same-depth nodes'),
        ('ROOT partial B-tree','partition\nonly tuples with prefix_len = 0')],
        'SQL plans may select different access paths; direct traversal explicitly uses the primary key.'))
    add(lanes('t08_lookup_order','The lookup B-tree groups user rows by route',[
        ('Partition 7; route 8A20...','Outside selected prefix 1001','gray','gray'),
        ('Partition 7; routes\n9001... / 95B0... / 9FFD...','Selected prefix 1001\nkeys K3 / K4 / K5 → application heap TIDs','amber','green'),
        ('Partition 7; route A01A...','Outside selected prefix 1001','gray','gray')],
        'Seek partition 7 and route ≥ 9000...; stop after 9FFF....',
        'Actual B-tree key order: partition(route), route bytes, original application key.'))
    add(lanes('t09_canonical_bytes','Canonical encoding separates row and key domains',[
        ('Full-row input header','ARIAMRKL + format version\n+ live-column count','green','green'),
        ('Each live row column','attnum + type OID + typmod + null flag\nif non-null: length + binary value'),
        ('Routing input header','ARIAROUT + format version\n+ indexed-key-column count','blue','blue'),
        ('Each indexed key column','key position + type OID + typmod\n+ null flag + optional length/value')],
        '32-bit format fields are big-endian; values use PostgreSQL type binary-send functions.',
        'Dropped user columns are skipped. Physical ctid/xmin are not included.'))
    add(lanes('t10_composite_keys','Composite routing preserves index-key order',[
        ('Merkle key','(warehouse_id, district_id, customer_id)'),
        ('partition_key_columns = 0','Hash the complete key\nroute value mod P selects partition'),
        ('partition_key_columns = 1','Hash warehouse_id to choose a group\nfull key chooses a partition inside it')],
        'Both routes still use the full adjusted route bytes for prefix traversal.'))
    add(tree('t11_partition_groups','Leading keys share a group of partitions',
        'Example: P = 8, s = 2\nleading key hashes to group 2',
        [('Group 0\np0, p1','gray'),('Group 1\np2, p3','gray'),('Group 2\np4, p5','amber'),('Group 3\np6, p7','gray')],
        'All rows with that leading key select p4 or p5 using their full-key digest.'))
    add(lanes('t12_hash_equations','The same additive rule applies at every level',[
        ('Leaf bucket','SUM h(application rows in bucket) mod 2²⁵⁶'),
        ('Internal node','SUM hash(direct children) mod 2²⁵⁶'),
        ('Partition root','SUM h(rows assigned to partition) mod 2²⁵⁶'),
        ('Read-time table digest','SUM hash(partition roots) mod 2²⁵⁶')],
        'Internal counts sum child counts. Node geometry does not enter the additive hash.'))
    add(pipeline('t13_arithmetic','A small arithmetic example shows the signs',[
        ('Initial bucket\nH = 100; N = 5','gray'),
        ('INSERT hash 7\nH = 107; N = 6','green'),
        ('DELETE hash 9\nH = 98; N = 5','red'),
        ('UPDATE hash 4 → 11\nH = 105; N = 5','amber')],
        'Invented small numbers for teaching; actual 256-bit sums wrap modulo 2²⁵⁶.'))
    add(panels('t14_delete_before_after','DELETE removes one contribution from a bucket',
        ['BEFORE DELETE','AFTER COMMIT'],[
        ('Rows A, B, C','Rows A, C'),
        ('H = hA + hB + hC\nN = 3','H = hA + hC\nN = 2')],
        'Net change: −hB and −1; every ancestor receives the same change.'))
    add(lanes('t15_delta_memory','What transaction-local staging stores',[
        ('MerkleSubxactFrame list','One frame for outer transaction\nseparate frames for SAVEPOINTs'),
        ('Each frame owns a PostgreSQL HTAB','Key: index OID + relfilenode\nevent type + old/new 8-byte route'),
        ('MerkleDeltaEntry value','xor_delta: signed 32-byte additive change\ncount_delta: int32 net row-count change')],
        'The historical xor_delta name does not mean the operation is XOR.'))
    add(pipeline('t16_hot_chain','A node HOT update can reuse its B-tree entries',[
        ('Node B-tree entry\nunchanged logical key','amber'),
        'Old node heap tuple\nprevious hash/count',
        ('HOT successor tuple\nnew hash/count','green')],
        'Hash/count are not node B-tree key columns. Node-table fillfactor is 80.',
        'HOT also needs suitable heap-page space; otherwise PostgreSQL maintains new index entries.'))
    f=Figure('t17_split_tree','Before / after: a leaf becomes an internal node',
        'Teaching configuration: fanout 4, split threshold 4, merge threshold 1.',1050,
        'Consistent-bucket example: the stored count/hash already agree with the heap. Empty child buckets are created.')
    f.box('before','BEFORE\nPrefix 10: leaf; N = 5\nrows A, B, C, D, E',535,200,530,160,'amber')
    f.box('after','AFTER\nPrefix 10: internal; N = 5\nH = hA + hB + hC + hD + hE',535,485,530,160,'green')
    f.edge('before','after')
    for i,label in enumerate(['1000\nA, B; N = 2','1001\nC; N = 1','1010\nD, E; N = 2','1011\nempty; N = 0']):
        f.box(str(i),label,70+i*380,790,320,140,'green')
    f.branch('after',[str(i) for i in range(4)],715)
    add(f)
    add(lanes('t18_fanout','Fanout controls bits consumed and children created',[
        ('Fanout 4','Consume 2 bits\n00 ... 11 → 4 child buckets'),
        ('Fanout 32','Consume 5 bits\n00000 ... 11111 → 32 child buckets'),
        ('Fanout 1024','Consume 10 bits\n0000000000 ... 1111111111 → 1024 children')],
        'Current uncommitted working tree accepts 1024; committed a7fc519d validates at most 256.',
        'Higher fanout creates more children per split; thresholds and row distribution also determine depth.'))
    f=Figure('t19_merge_tree','Before / after: sibling leaves collapse into a bucket',
        'Teaching configuration: fanout 4, merge threshold 3, split threshold greater than 3.',1050,
        'Delete child-node tuples; retain the parent identity and store the same combined hash/count.')
    f.box('before','BEFORE\nPrefix 10: internal; N = 3',535,195,530,130,'gray')
    for i,label in enumerate(['1000\nA; N = 1','1001\nB; N = 1','1010\nC; N = 1','1011\nempty; N = 0']):
        f.box(str(i),label,70+i*380,450,320,140,'green')
    f.branch('before',[str(i) for i in range(4)],385)
    f.box('after','AFTER\nPrefix 10: leaf; rows A, B, C\nH = hA + hB + hC; N = 3',435,780,730,160,'amber')
    f.branch('after',[str(i) for i in range(4)],685,collect=True)
    add(f)
    add(panels('t20_hysteresis','Identical rows need not produce identical topology',
        ['BUILD DIRECTLY AT N = 20','GROW PAST 32, SHRINK TO 20'],[
        ('S = 32; M = 8\nroot can remain a leaf','S = 32; M = 8\nroot may retain child buckets'),
        ('Final row population = 20','Final row population = 20'),
        ('Additive root = SUM row hashes','Same additive root\nmerge threshold is not reached')],
        'Splits and merges use different thresholds; shape can depend on history.'))
    add(lanes('t21_verification_boundary','Verification has a precise boundary',[
        ('CHECKED','Stored root hash sum equals\nindependently hashed heap-row sum','green','green'),
        ('CHECKED','Root counts equal heap count\ninternal nodes equal child totals','green','green'),
        ('NOT independently reconstructed','Exact routed membership of\nevery heap row in its stored bucket','gray','amber')],
        'A globally matching additive sum is not a complete independent bucket-localization audit.'))
    add(panels('t22_repair_leaf','A differing leaf identifies a user-row route range',
        ['REFERENCE PARTITION 7','LOCAL PARTITION 7'],[
        ('Root hash R','Root hash R′: mismatch'),
        ('Leaf hashes\n00: a; 01: b; 10: c; 11: d','Leaf hashes\n00: a; 01: b′; 10: c; 11: d'),
        ('Select leaf prefix 01','Route range\n4000000000000000 ... 7FFFFFFFFFFFFFFF')],
        'Range predicate also requires partition = 7; fetch actual application rows for repair.'))
    add(tree('t23_tpcc_storage','Each configured TPC-C table has the same families',
        'Application table T\nactual TPC-C user rows',
        [('Application indexes\nunique / query B-trees','blue'),
         ('T_merkle_idx\ncustom AM + metapage','green'),
         ('T_merkle_lookup_idx\npartition / route / key B-tree','amber')],
        'Dedicated node heap: ariabc_internal.merkle_node_T',
        'Internal node storage has three B-trees. The setup\'s application pk_* indexes are unique indexes, not PK constraints.'))
    add(lanes('t24_count_rules','Direct updates clamp counts and clear empty hashes',[
        ('Calculated count = old count + count delta','If negative: clamp to zero\nno inconsistency error here','gray','amber'),
        ('Resulting stored count = 0','Store an all-zero hash\ndiscard any residual contribution','gray','amber'),
        ('Resulting stored count > 0','Add the signed hash delta\nto the previous hash','gray','green')],
        'This describes the direct updater; it does not prove the corresponding heap bucket is empty.',
        'SPI leaf fallback instead requires a nonnegative count result and also clears the hash at zero.'))
    add(lanes('t25_split_rebuild','Split rebuilds local sums, not ancestor state',[
        ('Stored node agrees with scanned rows','Child buckets and replacement node\nrepresent the same total contributions','gray','green'),
        ('Nonzero scan count differs from positive target','Rebuild node from scanned rows\nlog count mismatch at DEBUG1','gray','amber'),
        ('Only the stored hash disagrees','Rebuild node from scanned row hashes\nno hash-mismatch check / message here','gray','amber'),
        ('Heap range scan returns zero rows','Return without splitting\nor rebuilding the stored node','gray','gray')],
        'A nonempty successful split replaces the splitting node\'s count and hash using the heap scan.',
        'Ancestors receive no reconciliation delta; do not interpret this as a full-tree repair.'))
    add(lanes('t26_repair_routes','Repair decisions choose rows, partitions, or a full copy',[
        ('Ineligible table, present locally','Whole-table checksum\ncopy only if it differs','gray','amber'),
        ('Roots match; heap verify off or passes','Return without repair','gray','green'),
        ('Roots match; enabled heap verify fails','Full-table copy','gray','red'),
        ('Roots differ; differing leaf ranges exist','Selective repair of those ranges','gray','green'),
        ('Roots differ; no leaf range differs','Whole-partition ranges\nsame selective row-repair machinery','gray','amber'),
        ('Post-selective roots or enabled heap verify fail','Full-table copy','gray','red')],
        'Selective eligibility: one Merkle key column + node table exists + application primary-key constraint.',
        'Operational errors can return failure. Full copy rebuilds Merkle first, then DELETE + COPY commits.'))
    add(lanes('t27_tpcc_repair','The inspected TPC-C schema uses checksum-based repair',[
        ('Seven tables have composite Merkle keys','They fail the single-key eligibility condition','gray','amber'),
        ('warehouse and item use one Merkle key column','Their pk_* indexes are unique indexes\nwithout primary-key constraints','gray','amber'),
        ('All nine tables, when present locally','Compare whole-table checksums\nmatch → no copy; mismatch → full copy','gray','green'),
        ('Schema changed by adding real PK constraints','Eligibility may change for single-key tables\nrecheck the actual schema','gray','blue')],
        'Source conclusion for the inspected dump → scale → index setup; no live database was inspected.',
        'Default geometry is 32 / 1024 / 256. Having Merkle nodes alone does not enable selective repair.'))
    for f in out:
        if f.name in {'m15_same_leaf','m18_move_same_partition','m19_move_partition','m23_shared_ancestor'}:
            # Contributions flow upward into their combined sum.
            f.edges=[(b,a,list(reversed(p)) if p else None,c,d) for a,b,p,c,d in f.edges]
        if f.name == 'm02_forest':
            # The global digest is computed, not a stored parent node.
            f.edges=[(a,b,p,c,a=='root') for a,b,p,c,d in f.edges]
        if f.name == 't11_partition_groups':
            # Only group 2 is selected in this illustrative example.
            f.edges=[e for e in f.edges if e[1]=='c2']
    return out


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    args.out.mkdir(parents=True,exist_ok=True)
    for f in figures():
        f.render(args.out)
        print(f.name,flush=True)
    if ERRORS:
        raise RuntimeError('\n'.join(ERRORS))
    (args.out/'figures.json').write_text(json.dumps(MANIFEST,indent=2)+'\n')
    # QA contact sheets, kept outside the document's image directory.
    qa=args.out.parent/'qa'
    qa.mkdir(exist_ok=True)
    for start in range(0,len(MANIFEST),4):
        group=MANIFEST[start:start+4]
        sheet=Image.new('RGB',(2000,1900),'#e8edf2')
        for j,entry in enumerate(group):
            im=Image.open(args.out/(entry['name']+'.png'))
            im.thumbnail((970,875))
            x=15+(j%2)*1000+(970-im.width)//2
            y=45+(j//2)*950
            sheet.paste(im,(x,y))
            ImageDraw.Draw(sheet).text((15+(j%2)*1000,15+(j//2)*950),entry['name'],fill=INK,
                font=ImageFont.truetype(BOLD,21))
        sheet.save(qa/f'contact_{start//4+1:02d}.png')
    print(f'PASS: {len(MANIFEST)} figures; labels fit; bounds checked.',flush=True)


if __name__=='__main__':
    main()
