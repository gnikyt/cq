(() => {
  "use strict";

  /* ---------------- Theme ---------------- */
  const root = document.documentElement;
  let stored = null;
  try {
    stored = localStorage.getItem("cq-theme");
  } catch {}
  if (stored) {
    root.setAttribute("data-theme", stored);
  } else if (window.matchMedia?.("(prefers-color-scheme: dark)").matches) {
    root.setAttribute("data-theme", "dark");
  }

  const toggleTheme = () => {
    const next = root.getAttribute("data-theme") === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try {
      localStorage.setItem("cq-theme", next);
    } catch {}
  };

  /* ---------------- Syntax highlighting ----------------
     Escape first, protect strings/comments, then colour the rest.
     Intentionally lightweight: good enough for Go, bash, and json snippets. */
  const GO_KEYWORDS = /\b(package|import|func|return|if|else|for|range|switch|case|default|select|go|defer|chan|map|struct|interface|type|var|const|break|continue|fallthrough|goto)\b/g;
  const GO_BUILTINS = /\b(nil|true|false|iota|make|new|len|cap|append|copy|delete|close|panic|recover|print|println|error)\b/g;
  const GO_TYPES = /\b(string|int|int8|int16|int32|int64|uint|uint8|uint16|uint32|uint64|uintptr|byte|rune|float32|float64|complex64|complex128|bool|any)\b/g;

  // Private-use sentinels wrap stashed strings/comments so later keyword,
  // number, and function passes can never match inside them.
  const OPEN = String.fromCharCode(0xe000);
  const CLOSE = String.fromCharCode(0xe001);
  const RESTORE = new RegExp(`${OPEN}([\\uE010-\\uE019]+)${CLOSE}`, "g");

  const escapeHtml = (s) =>
    s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

  const highlight = (code, lang) => {
    let escaped = escapeHtml(code);
    const store = [];

    // Encode each store index as private-use characters so later keyword,
    // number, and function passes never match the placeholder.
    const stash = (html) => {
      store.push(html);
      const token = String(store.length - 1)
        .split("")
        .map((d) => String.fromCharCode(0xe010 + Number(d)))
        .join("");
      return `${OPEN}${token}${CLOSE}`;
    };

    // Comments + strings first so keywords inside them are left alone.
    if (lang === "go" || lang === "bash" || lang === "" || lang === "text") {
      escaped = escaped.replace(/(\/\/[^\n]*|#[^\n]*)/g, (m) =>
        stash(`<span class="tok-comment">${m}</span>`)
      );
      escaped = escaped.replace(/\/\*[\s\S]*?\*\//g, (m) =>
        stash(`<span class="tok-comment">${m}</span>`)
      );
      escaped = escaped.replace(/("(?:[^"\\]|\\.)*"|`[^`]*`|'(?:[^'\\]|\\.)*')/g, (m) =>
        stash(`<span class="tok-string">${m}</span>`)
      );
    }

    if (lang === "json") {
      escaped = escaped.replace(/("(?:[^"\\]|\\.)*")(\s*:)?/g, (m, str, colon) => {
        const cls = colon ? "tok-func" : "tok-string";
        return stash(`<span class="${cls}">${str}</span>`) + (colon || "");
      });
      escaped = escaped.replace(/\b(true|false|null)\b/g, '<span class="tok-keyword">$1</span>');
      escaped = escaped.replace(/-?\b\d+(?:\.\d+)?\b/g, '<span class="tok-number">$&</span>');
    }

    if (lang === "go") {
      escaped = escaped
        .replace(GO_KEYWORDS, '<span class="tok-keyword">$1</span>')
        .replace(GO_TYPES, '<span class="tok-type">$1</span>')
        .replace(GO_BUILTINS, '<span class="tok-builtin">$1</span>')
        .replace(/\b([A-Za-z_]\w*)(\s*\()/g, (m, name, paren) => `<span class="tok-func">${name}</span>${paren}`)
        .replace(/\b(0x[0-9a-fA-F]+|\d+(?:\.\d+)?)\b/g, '<span class="tok-number">$1</span>');
    }

    // Restore protected spans.
    return escaped.replace(RESTORE, (m, token) => {
      const idx = token
        .split("")
        .map((c) => c.charCodeAt(0) - 0xe010)
        .join("");
      return store[Number(idx)];
    });
  };

  const COPY_ICON =
    '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><rect x="5.5" y="5.5" width="8" height="8" rx="1.5"/><path d="M10.5 5.5V3.5a1 1 0 0 0-1-1h-6a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h2"/></svg>';

  const enhanceCodeBlocks = () => {
    for (const code of document.querySelectorAll("pre > code")) {
      const pre = code.parentNode;
      let lang = "";
      for (const c of (code.className || "").split(/\s+/)) {
        if (c.startsWith("language-")) lang = c.slice(9);
      }

      const raw = code.textContent.replace(/\n$/, "");
      code.innerHTML = highlight(raw, lang);

      const wrap = document.createElement("div");
      wrap.className = "code-block";
      pre.parentNode.insertBefore(wrap, pre);

      const head = document.createElement("div");
      head.className = "code-head";
      head.innerHTML =
        `<span class="code-lang">${lang || "text"}</span>` +
        `<button class="copy-btn" type="button">${COPY_ICON}<span>Copy</span></button>`;
      wrap.appendChild(head);
      wrap.appendChild(pre);

      const btn = head.querySelector(".copy-btn");
      btn.addEventListener("click", () => {
        navigator.clipboard.writeText(raw).then(() => {
          btn.classList.add("copied");
          btn.querySelector("span").textContent = "Copied";
          setTimeout(() => {
            btn.classList.remove("copied");
            btn.querySelector("span").textContent = "Copy";
          }, 1600);
        });
      });
    }
  };

  /* ---------------- Heading anchors + TOC ---------------- */
  const slugify = (t) =>
    t.toLowerCase().trim().replace(/[^\w\s-]/g, "").replace(/\s+/g, "-");

  const buildAnchorsAndToc = () => {
    const content = document.querySelector(".content-inner");
    const tocList = document.querySelector(".toc ul");
    if (!content) return [];

    const entries = [];
    for (const h of content.querySelectorAll("h2, h3")) {
      if (!h.id) h.id = slugify(h.textContent);

      const anchor = document.createElement("a");
      anchor.className = "anchor";
      anchor.href = `#${h.id}`;
      anchor.setAttribute("aria-label", "Link to this section");
      anchor.textContent = "#";
      h.appendChild(anchor);

      if (tocList) {
        const li = document.createElement("li");
        const link = document.createElement("a");
        link.href = `#${h.id}`;
        link.textContent = h.textContent.replace(/#$/, "").trim();
        link.className = h.tagName === "H3" ? "h3" : "h2";
        li.appendChild(link);
        tocList.appendChild(li);
        entries.push({ id: h.id, link, el: h });
      }
    }
    return entries;
  };

  /* ---------------- Scrollspy ---------------- */
  const initScrollspy = (entries) => {
    if (!entries.length || !("IntersectionObserver" in window)) return;

    const visible = new Set();
    const obs = new IntersectionObserver(
      (records) => {
        for (const r of records) {
          if (r.isIntersecting) visible.add(r.target.id);
          else visible.delete(r.target.id);
        }
        const current = entries.find((e) => visible.has(e.id))?.id ?? null;
        for (const e of entries) {
          e.link.classList.toggle("active", e.id === current);
        }
      },
      { rootMargin: "-72px 0px -70% 0px", threshold: 0 }
    );
    for (const e of entries) obs.observe(e.el);
  };

  /* ---------------- Mobile nav ---------------- */
  const initNav = () => {
    const menuBtn = document.querySelector(".menu-btn");
    const backdrop = document.querySelector(".backdrop");
    const close = () => document.body.classList.remove("nav-open");

    menuBtn?.addEventListener("click", () => document.body.classList.toggle("nav-open"));
    backdrop?.addEventListener("click", close);
    for (const a of document.querySelectorAll(".sidebar a")) {
      a.addEventListener("click", close);
    }
  };

  /* ---------------- Boot ---------------- */
  document.addEventListener("DOMContentLoaded", () => {
    document.querySelector(".theme-toggle")?.addEventListener("click", toggleTheme);
    enhanceCodeBlocks();
    initScrollspy(buildAnchorsAndToc());
    initNav();
  });
})();
