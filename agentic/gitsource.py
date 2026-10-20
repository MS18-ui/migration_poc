"""Read legacy jobs from a git repository (for the UI). Read-only: nothing from the repo is ever executed.

Safeguards: https URLs only (no credentials in the URL, no ssh/file/ext schemes), shallow single-branch clone with
hooks irrelevant (git never copies hooks) and symlinks disabled, a clone timeout, a size cap, a cap on listed files,
and every later file access is resolved and must stay inside the clone. An access token is passed to git through the
environment (not the command line) and is scrubbed from any error text.
"""
import base64
import os
import re
import shutil
import subprocess
import tempfile
from typing import List, Optional, Tuple

SOURCE_EXTS = (".py", ".xml", ".sh", ".hql", ".sql", ".hive", ".xlsx", ".xls")
MAX_REPO_BYTES = 150 * 1024 * 1024
MAX_FILE_BYTES = 5 * 1024 * 1024
MAX_LISTED = 500
_SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", ".ipynb_checkpoints", "target", "build", "dist"}
_URL_RE = re.compile(r"^https://[A-Za-z0-9.-]+(?::\d+)?/[A-Za-z0-9._~%+/-]+$")
_REF_RE = re.compile(r"^[A-Za-z0-9._/-]{1,100}$")


class GitSourceError(Exception):
    pass


def validate_repo_url(url: str) -> str:
    url = (url or "").strip().rstrip("/")
    if not _URL_RE.match(url) or ".." in url:
        raise GitSourceError("Use a plain https repository URL such as https://github.com/org/repo.git "
                             "(no credentials in the URL, no ssh or file URLs).")
    return url


def _scrub(text: str, token: Optional[str]) -> str:
    if token:
        text = text.replace(token, "<token>").replace(base64.b64encode(f"x-access-token:{token}".encode()).decode(), "<token>")
    return text


def safe_join(root: str, rel: str) -> str:
    """Resolve `rel` under `root`; reject anything that escapes it (.., absolute paths, symlinks)."""
    root_real = os.path.realpath(root)
    path = os.path.realpath(os.path.join(root_real, rel.replace("\\", "/").lstrip("/")))
    if path != root_real and not path.startswith(root_real + os.sep):
        raise GitSourceError(f"path escapes the repository: {rel}")
    return path


def _dir_size(path: str) -> int:
    total = 0
    for dp, dn, fn in os.walk(path):
        dn[:] = [d for d in dn if d != ".git"]
        for f in fn:
            try:
                total += os.path.getsize(os.path.join(dp, f))
            except OSError:
                pass
    return total


def clone_repo(url: str, ref: Optional[str] = None, token: Optional[str] = None,
               dest_parent: Optional[str] = None, timeout: int = 180) -> Tuple[str, str]:
    """Shallow-clone into a fresh temp folder. Returns (clone_root, short_commit). Caller should cleanup(root)."""
    git = shutil.which("git")
    if not git:
        raise GitSourceError("git is not installed on the machine running the UI.")
    url = validate_repo_url(url)
    ref = (ref or "").strip() or None
    if ref and (not _REF_RE.match(ref) or ref.startswith("-")):
        raise GitSourceError("The branch/tag name contains characters that are not allowed. (Commit hashes cannot be cloned shallowly; use a branch or tag.)")
    parent = tempfile.mkdtemp(prefix="gitsrc_", dir=dest_parent)
    root = os.path.join(parent, "repo")
    cmd = [git, "-c", "core.symlinks=false", "clone", "--depth", "1", "--single-branch", "--no-tags"]
    if ref:
        cmd += ["--branch", ref]
    cmd += ["--", url, root]
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
    if token:
        basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader", GIT_CONFIG_VALUE_0=f"Authorization: Basic {basic}")
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    except subprocess.TimeoutExpired:
        shutil.rmtree(parent, ignore_errors=True)
        raise GitSourceError(f"git clone timed out after {timeout}s")
    if p.returncode != 0:
        shutil.rmtree(parent, ignore_errors=True)
        raise GitSourceError("git clone failed: " + _scrub((p.stderr or p.stdout or "").strip(), token)[-400:])
    if _dir_size(root) > MAX_REPO_BYTES:
        shutil.rmtree(parent, ignore_errors=True)
        raise GitSourceError(f"repository is larger than {MAX_REPO_BYTES // (1024 * 1024)} MB; point at a smaller repository")
    r = subprocess.run([git, "-C", root, "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=30)
    return root, (r.stdout.strip() or "unknown")


def list_sources(root: str, subpath: str = "", max_files: int = MAX_LISTED) -> List[str]:
    """Candidate legacy job files (relative, posix style), skipping build/VCS folders and oversized files."""
    base = safe_join(root, subpath or "")
    root_real = os.path.realpath(root)
    found: List[str] = []
    for dp, dn, fn in os.walk(base):
        dn[:] = sorted(d for d in dn if d not in _SKIP_DIRS and not d.startswith("."))
        for f in sorted(fn):
            if f.lower().endswith(SOURCE_EXTS):
                full = os.path.join(dp, f)
                try:
                    if os.path.getsize(full) > MAX_FILE_BYTES:
                        continue
                except OSError:
                    continue
                found.append(os.path.relpath(full, root_real).replace(os.sep, "/"))
                if len(found) >= max_files:
                    return found
    return found


def read_text(root: str, rel: str, max_bytes: int = MAX_FILE_BYTES) -> str:
    path = safe_join(root, rel)
    if os.path.getsize(path) > max_bytes:
        raise GitSourceError(f"{rel} is larger than {max_bytes // 1024} KB")
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def copy_file(root: str, rel: str, dest_dir: str) -> str:
    """Copy one repo file into `dest_dir` (so a job does not depend on the temp clone). Returns the new path."""
    src = safe_join(root, rel)
    os.makedirs(dest_dir, exist_ok=True)
    dst = os.path.join(dest_dir, os.path.basename(src))
    shutil.copyfile(src, dst)
    return dst


def cleanup(root: str) -> None:
    """Remove a clone made by clone_repo (its parent temp folder)."""
    parent = os.path.dirname(os.path.realpath(root))
    if os.path.basename(parent).startswith("gitsrc_"):
        shutil.rmtree(parent, ignore_errors=True)
