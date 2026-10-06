/* ============================================================
   Tickable progress checkboxes.

   MkDocs Material renders task lists ("- [ ] ...") as DISABLED
   checkboxes. This enables them, remembers what was ticked in
   localStorage, and shows a per-list counter.

   State is per-browser and per-page. Items are keyed by a hash of
   their label text, so reordering a list keeps ticks attached to
   the right item; editing an item's wording resets just that one.
   ============================================================ */

(function () {
  "use strict";

  var STORE = "openslava_lab_progress";

  function load() {
    try { return JSON.parse(localStorage.getItem(STORE) || "{}"); }
    catch (e) { return {}; }
  }

  function save(state) {
    try { localStorage.setItem(STORE, JSON.stringify(state)); }
    catch (e) { /* private mode, quota — ticking still works for this page view */ }
  }

  // Short, stable hash of the item's text.
  function hash(text) {
    var h = 5381;
    for (var i = 0; i < text.length; i++) {
      h = ((h << 5) + h + text.charCodeAt(i)) | 0;
    }
    return "i" + (h >>> 0).toString(36);
  }

  function keyFor(item) {
    var text = (item.textContent || "").replace(/\s+/g, " ").trim().slice(0, 160);
    return location.pathname + "#" + hash(text);
  }

  function init() {
    var state = load();
    var lists = document.querySelectorAll(".md-typeset .task-list");
    if (!lists.length) return;

    Array.prototype.forEach.call(lists, function (list) {
      if (list.getAttribute("data-progress-init")) return;   // already wired
      list.setAttribute("data-progress-init", "1");

      var items = list.querySelectorAll(".task-list-item");
      var boxes = [];

      Array.prototype.forEach.call(items, function (item) {
        var box = item.querySelector('input[type="checkbox"]');
        if (!box) return;

        box.disabled = false;
        box.removeAttribute("disabled");
        box.style.cursor = "pointer";
        item.classList.add("is-tickable");

        var key = keyFor(item);
        if (state[key]) {
          box.checked = true;
          item.classList.add("is-done");
        }

        box.addEventListener("change", function () {
          var s = load();
          if (box.checked) { s[key] = 1; item.classList.add("is-done"); }
          else { delete s[key]; item.classList.remove("is-done"); }
          save(s);
          update();
        });

        boxes.push(box);
      });

      if (!boxes.length) return;

      // Counter + reset, inserted above the list.
      var bar = document.createElement("div");
      bar.className = "progress-bar-row";
      var label = document.createElement("span");
      label.className = "progress-count";
      var reset = document.createElement("button");
      reset.type = "button";
      reset.className = "progress-reset";
      reset.textContent = "Clear";
      reset.addEventListener("click", function () {
        var s = load();
        boxes.forEach(function (b, i) {
          b.checked = false;
          delete s[keyFor(items[i])];
          items[i].classList.remove("is-done");
        });
        save(s);
        update();
      });
      bar.appendChild(label);
      bar.appendChild(reset);
      list.parentNode.insertBefore(bar, list);

      function update() {
        var done = boxes.filter(function (b) { return b.checked; }).length;
        label.textContent = done + " of " + boxes.length + " done";
        bar.classList.toggle("is-complete", done === boxes.length);
      }

      update();
      list._update = update;
    });

    // Keep every counter on the page in sync.
    function update() {
      Array.prototype.forEach.call(lists, function (l) {
        if (l._update) l._update();
      });
    }
  }

  // Run now (or at DOM ready) ...
  if (document.readyState !== "loading") {
    init();
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }

  // ... and again on every instant-navigation page swap. Material defines
  // document$ in its own bundle, which may load after this file, so look for
  // it lazily rather than assuming it exists right now.
  function subscribeWhenReady(attempt) {
    if (typeof window.document$ !== "undefined" && window.document$.subscribe) {
      window.document$.subscribe(function () { init(); });
    } else if (attempt < 20) {
      setTimeout(function () { subscribeWhenReady(attempt + 1); }, 100);
    }
  }
  subscribeWhenReady(0);
})();
