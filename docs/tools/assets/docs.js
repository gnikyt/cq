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

  // Ensure a heading has an id and a hover "#" anchor. When prefix is given
  // (single-page build) the id is namespaced so it stays unique across docs.
  const ensureAnchor = (h, prefix) => {
    if (prefix) h.id = `${prefix}--${slugify(h.textContent)}`;
    else if (!h.id) h.id = slugify(h.textContent);

    if (!h.querySelector(".anchor")) {
      const anchor = document.createElement("a");
      anchor.className = "anchor";
      anchor.href = `#${h.id}`;
      anchor.setAttribute("aria-label", "Link to this section");
      anchor.textContent = "#";
      h.appendChild(anchor);
    }
  };

  // Collapsed by default; reveals the "Show all" toggle only when the TOC
  // overflows its column. Expanding swaps the fade for a real scrollbar.
  const setTocToggleLabel = (toggle, expanded) => {
    const label = toggle.querySelector(".toc__toggle-label");
    if (label) label.textContent = expanded ? "Show less ⌃" : "Show all ⌄";
  };

  const updateTocOverflow = () => {
    const toc = document.querySelector(".toc");
    const scroll = toc?.querySelector(".toc__scroll");
    const toggle = toc?.querySelector(".toc__toggle");
    if (!toc || !scroll || !toggle) return;
    toc.classList.remove("expanded");
    toggle.setAttribute("aria-expanded", "false");
    setTocToggleLabel(toggle, false);
    toggle.hidden = scroll.scrollHeight <= scroll.clientHeight + 1;
  };

  const initTocToggle = () => {
    const toc = document.querySelector(".toc");
    const toggle = toc?.querySelector(".toc__toggle");
    toggle?.addEventListener("click", () => {
      const expanded = toc.classList.toggle("expanded");
      toggle.setAttribute("aria-expanded", String(expanded));
      setTocToggleLabel(toggle, expanded);
    });
    window.addEventListener("resize", updateTocOverflow, { passive: true });
  };

  // Populate the on-page TOC from a root element's headings; returns entries.
  const buildToc = (root) => {
    const tocList = document.querySelector(".toc ul");
    if (tocList) tocList.innerHTML = "";
    const entries = [];
    if (!root) return entries;

    for (const h of root.querySelectorAll("h2, h3")) {
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
    const toc = document.querySelector(".toc");
    if (toc) toc.style.display = entries.length ? "" : "none";
    updateTocOverflow();
    return entries;
  };

  /* ---------------- Scrollspy ----------------
     Highlights the last heading scrolled past the header line, so there is
     always exactly one active entry. Returns a teardown function. */
  const HEADER_OFFSET = 88;

  const initScrollspy = (entries) => {
    if (!entries.length) return () => {};

    const onScroll = () => {
      let current = entries[0].id;
      for (const e of entries) {
        if (e.el.getBoundingClientRect().top <= HEADER_OFFSET) current = e.id;
        else break;
      }
      for (const e of entries) e.link.classList.toggle("active", e.id === current);
    };

    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
    return () => window.removeEventListener("scroll", onScroll);
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

  /* ---------------- Multi-page mode ---------------- */
  const initMultiPage = () => {
    const content = document.querySelector(".content-inner");
    if (!content) return;
    for (const h of content.querySelectorAll("h2, h3")) ensureAnchor(h);
    initScrollspy(buildToc(content));
  };

  /* ---------------- Single-page mode (client-side router) ----------------
     All docs live in the DOM as .doc-section; only the routed one is shown. */
  const initSinglePage = (sections) => {
    for (const s of sections) {
      for (const h of s.querySelectorAll("h2, h3")) ensureAnchor(h, s.id);
    }

    const sidebarLinks = [...document.querySelectorAll(".sidebar a")];
    const known = (id) => sections.some((s) => s.id === id);
    let activeId = null;
    let spy = null;

    const show = (id) => {
      if (!known(id)) id = "top";
      if (id === activeId) return;
      const isInitial = activeId === null;

      for (const s of sections) s.hidden = s.id !== id;
      for (const a of sidebarLinks) {
        a.classList.toggle("active", a.getAttribute("href") === `#${id}`);
      }
      const active = document.getElementById(id);
      spy?.();
      spy = initScrollspy(buildToc(active));
      activeId = id;
      window.scrollTo(0, 0);

      // Move focus to the doc heading so keyboard/screen-reader users follow
      // the switch (but not on first load, which would steal initial focus).
      if (!isInitial) {
        const heading = active.querySelector("h1, h2");
        if (heading) {
          heading.setAttribute("tabindex", "-1");
          heading.focus({ preventScroll: true });
        }
      }
    };

    const route = () => {
      const raw = decodeURIComponent(location.hash.slice(1));
      if (raw.includes("--")) {
        const docId = raw.split("--")[0];
        if (known(docId)) {
          show(docId);
          document.getElementById(raw)?.scrollIntoView();
        }
        return;
      }
      // Empty or known doc id routes; unknown hashes (e.g. the skip link) are
      // left alone so they cannot hijack the router.
      if (raw === "" || known(raw)) show(raw || "top");
    };

    window.addEventListener("hashchange", route);
    route();
    if (!activeId) show("top");
  };

  /* ---------------- Boot ---------------- */
  document.addEventListener("DOMContentLoaded", () => {
    document.querySelector(".theme-toggle")?.addEventListener("click", toggleTheme);
    enhanceCodeBlocks();
    initTocToggle();
    const sections = [...document.querySelectorAll(".doc-section")];
    if (sections.length) initSinglePage(sections);
    else initMultiPage();
    initNav();
  });
})();
