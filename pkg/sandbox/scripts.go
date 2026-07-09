package sandbox

// This file holds the python3 helper programs that the FileOps methods run
// inside the sandbox. Each reads a JSON args object from stdin (string fields are
// base64-encoded to dodge shell/encoding hazards) and prints a single JSON object
// to stdout describing the outcome. Keeping them here, isolated from the Go glue,
// makes the derive-from-Execute contract easy to audit.

const lsScript = `
import os, sys, json, base64
args = json.load(sys.stdin)
path = base64.b64decode(args["path"]).decode()
try:
    entries = []
    for name in sorted(os.listdir(path)):
        full = os.path.join(path, name)
        try:
            st = os.stat(full)
            entries.append({"name": name, "is_dir": os.path.isdir(full), "size": st.st_size})
        except OSError:
            entries.append({"name": name, "is_dir": False, "size": 0})
    print(json.dumps({"ok": True, "entries": entries}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
`

const readScript = `
import sys, json, base64
args = json.load(sys.stdin)
path = base64.b64decode(args["path"]).decode()
offset = int(args["offset"]); limit = int(args["limit"]); maxbytes = int(args["max_bytes"])
try:
    with open(path, "rb") as f:
        raw = f.read()
    text = raw.decode("utf-8", "replace")
    lines = text.splitlines(keepends=True)
    start = offset if offset > 0 else 0
    end = len(lines) if limit <= 0 else start + limit
    sel = "".join(lines[start:end]).encode("utf-8")
    truncated = False
    if maxbytes > 0 and len(sel) > maxbytes:
        sel = sel[:maxbytes]; truncated = True
    print(json.dumps({"ok": True, "content_b64": base64.b64encode(sel).decode(), "truncated": truncated}))
except FileNotFoundError:
    print(json.dumps({"ok": False, "error": "no such file"}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
`

const writeScript = `
import os, sys, json, base64
args = json.load(sys.stdin)
path = base64.b64decode(args["path"]).decode()
content = base64.b64decode(args["content"])
try:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    print(json.dumps({"ok": True}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
`

const editScript = `
import sys, json, base64
args = json.load(sys.stdin)
path = base64.b64decode(args["path"]).decode()
old = base64.b64decode(args["old"]).decode("utf-8", "replace")
new = base64.b64decode(args["new"]).decode("utf-8", "replace")
try:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    count = text.count(old)
    if count == 0:
        print(json.dumps({"ok": False, "code": "not_found"}))
    elif count > 1:
        print(json.dumps({"ok": False, "code": "ambiguous", "count": count}))
    else:
        text = text.replace(old, new, 1)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        print(json.dumps({"ok": True}))
except FileNotFoundError:
    print(json.dumps({"ok": False, "code": "error", "error": "no such file"}))
except Exception as e:
    print(json.dumps({"ok": False, "code": "error", "error": str(e)}))
`

const deleteScript = `
import os, sys, json, base64, shutil
args = json.load(sys.stdin)
path = base64.b64decode(args["path"]).decode()
try:
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path)
    else:
        os.remove(path)
    print(json.dumps({"ok": True}))
except FileNotFoundError:
    print(json.dumps({"ok": False, "error": "no such file"}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
`

const globScript = `
import sys, json, base64, glob
args = json.load(sys.stdin)
pattern = base64.b64decode(args["pattern"]).decode()
maxres = int(args["max_results"])
try:
    res = sorted(glob.glob(pattern, recursive=True))[:maxres]
    print(json.dumps({"ok": True, "paths": res}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
`

const grepScript = `
import os, sys, json, base64, re, shutil, subprocess
args = json.load(sys.stdin)
pattern = base64.b64decode(args["pattern"]).decode()
path = base64.b64decode(args["path"]).decode()
maxmatches = int(args["max_matches"])
matches = []

def add(p, n, line):
    if len(matches) >= maxmatches:
        return False
    matches.append({"path": p, "line_number": n, "line": line[:2000]})
    return True

try:
    rg = shutil.which("rg")
    if rg:
        proc = subprocess.run(
            [rg, "--with-filename", "--line-number", "--no-heading", "--color", "never", pattern, path],
            capture_output=True, text=True)
        for l in proc.stdout.splitlines():
            parts = l.split(":", 2)
            if len(parts) < 3:
                continue
            try:
                n = int(parts[1])
            except ValueError:
                continue
            if not add(parts[0], n, parts[2]):
                break
    else:
        reg = re.compile(pattern)
        def grep_file(fp):
            try:
                with open(fp, "r", errors="replace") as f:
                    for i, line in enumerate(f, 1):
                        if reg.search(line):
                            if not add(fp, i, line.rstrip("\n")):
                                return False
            except (OSError, UnicodeError):
                pass
            return True
        if os.path.isdir(path):
            done = False
            for root, dirs, files in os.walk(path):
                for name in sorted(files):
                    if not grep_file(os.path.join(root, name)):
                        done = True
                        break
                if done:
                    break
        else:
            grep_file(path)
    print(json.dumps({"ok": True, "matches": matches}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}))
`
