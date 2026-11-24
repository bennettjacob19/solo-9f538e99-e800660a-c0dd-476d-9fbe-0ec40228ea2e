(() => {
  const $ = (id) => document.getElementById(id);
  const form = $("drill-form");
  const notice = $("notice");
  let pollTimer = null;

  const fmt = (n) => {
    if (n === null || n === undefined) return "-";
    if (typeof n !== "number") return String(n);
    return n.toLocaleString("en-US");
  };

  const pct = (part, total) => {
    if (!total) return 0;
    return Math.max(0, Math.min(100, (part / total) * 100));
  };

  function showNotice(text, kind = "error") {
    notice.hidden = false;
    notice.className = `notice ${kind}`;
    notice.textContent = text;
  }

  function clearNotice() {
    notice.hidden = true;
    notice.textContent = "";
  }

  function formPayload() {
    const data = new FormData(form);
    const num = (name) => Number(data.get(name));
    return {
      size_bytes: num("size_bytes"),
      chunk_size: num("chunk_size"),
      chunk_delay_ms: num("chunk_delay_ms"),
      retention_bytes: num("retention_bytes"),
      retention_memory_bytes: num("retention_memory_bytes"),
      backfill_bytes: num("backfill_bytes"),
      pause_fraction: Number(data.get("pause_fraction")),
      auto_switch_on_pause: data.get("auto_switch_on_pause") === "on",
      switch_version: "v2",
    };
  }

  function verifyClass(result) {
    if (result === "match") return "match";
    if (result === "mismatch" || result === "incomplete") return "incomplete";
    return "pending";
  }

  function renderVerify(el, v) {
    if (!v || !v.actual || v.result === "pending") {
      el.textContent = "待运行"; el.className = "pending"; el.title = ""; return;
    }
    const label = v.result === "match" ? "通过" : v.result === "incomplete" ? "未收齐" : "不匹配";
    el.textContent = `${label} · ${v.actual ? v.actual.slice(0, 16) + "…" : "-"}`;
    el.title = `expected ${v.expected || "-"}\nactual   ${v.actual || "-"}`;
    el.className = verifyClass(v.result);
  }

  function renderEvents(events) {
    const list = $("events");
    list.innerHTML = "";
    (events || []).slice(-80).forEach((e) => {
      const li = document.createElement("li");
      const t = document.createElement("span");
      t.className = "t";
      t.textContent = (e.at || "").slice(11, 23);
      const k = document.createElement("span");
      k.className = `kind ${e.kind === "error" ? "error" : ""}`;
      k.textContent = e.kind;
      const m = document.createElement("span");
      m.textContent = e.message;
      li.append(t, k, m);
      list.append(li);
    });
    list.scrollTop = list.scrollHeight;
  }

  function render(state) {
    const total = state.total_bytes || 0;
    $("phase").textContent = state.phase || "idle";
    $("outcome").textContent =
      state.outcome === "both_complete" ? "两路均收齐（both_complete）" :
      state.outcome === "slow_missing" ? "仅慢端缺段（slow_missing）" :
      state.outcome === "pending" ? "运行中/待运行" : (state.outcome || "-");
    $("total-bytes").textContent = total ? fmt(total) : "-";
    const main = state.main || {};
    $("main-etag").textContent = main.etag || "-";
    $("expected-sha").textContent = main.expected_sha256 || "-";
    $("elapsed").textContent = state.elapsed_ms ? `${fmt(state.elapsed_ms)} ms` : "-";

    const fast = state.fast || { received_bytes: 0 };
    const slow = state.slow || { received_bytes: 0 };
    $("fast-bytes").textContent = fmt(fast.received_bytes || 0);
    $("slow-bytes").textContent = fmt(slow.received_bytes || 0);
    $("fast-bar").style.width = `${pct(fast.received_bytes || 0, total)}%`;
    $("slow-bar").style.width = `${pct(slow.received_bytes || 0, total)}%`;
    renderVerify($("fast-verify"), fast.verification);
    renderVerify($("slow-verify"), slow.verification);

    const missing = slow.missing_segments || [];
    $("slow-missing").textContent = missing.length
      ? missing.map((s) => `${s.start}-${s.end} (${fmt(s.length)}B)`).join(", ")
      : "无";
    $("slow-pause").textContent = slow.pause_threshold_bytes
      ? `${fmt(slow.paused_at_bytes || 0)} / 阈值 ${fmt(slow.pause_threshold_bytes)}`
      : "-";
    $("slow-resumed").textContent = slow.resumed ? "已续传" : (slow.paused ? "已暂停" : "-");

    const bf = slow.backfill || {};
    $("backfill-verify").textContent = bf.attempted
      ? `ETag ${bf.etag_verified ? "✓" : "✗"} · 区间 ${bf.range_verified ? "✓" : "✗"}`
      : "-";
    $("backfill-status").textContent = bf.status || "-";
    $("backfill-quota").textContent = bf.quota_bytes !== undefined
      ? `${fmt(bf.quota_bytes - (bf.used_bytes || 0))} / ${fmt(bf.quota_bytes)}`
      : "-";
    const qc = state.quota_check;
    $("quota-cover").textContent = qc
      ? (qc.combined_covers_tail ? "是（两项额度够补尾段）" : "否（额度不足）")
      : "-";

    const origin = state.origin || {};
    $("origin-sent").textContent = fmt(origin.bytes_sent || 0);
    $("origin-reqs").textContent = `${fmt(origin.main_requests || 0)} / ${fmt(origin.range_requests || 0)}`;
    $("origin-version").textContent = origin.current_version || "-";
    if (origin.current_version) currentOriginVersion = origin.current_version;

    const workEl = $("origin-range-work");
    const reqs = Array.isArray(origin.requests) ? origin.requests : [];
    const ranged = reqs.find((r) => r.status === 206 && r.body_bytes_used !== undefined);
    if (ranged) {
      const used = fmt(ranged.body_bytes_used || 0);
      if (ranged.work_within_limit === true) {
        workEl.textContent = `${used} B · ≤1 MiB 预算 ✓`;
        workEl.className = "v mono match";
      } else if (ranged.work_within_limit === false) {
        workEl.textContent = `${used} B · 超出 1 MiB 预算 ✗`;
        workEl.className = "v mono mismatch";
      } else {
        workEl.textContent = `${used} B（区间 > 64 KiB，预算规则不适用）`;
        workEl.className = "v mono";
      }
      workEl.title =
        `range=${ranged.range || "-"} range_length=${ranged.range_length}\n` +
        `positioning=${ranged.positioning_bytes} read=${ranged.storage_read_bytes} ` +
        `generated=${ranged.generated_bytes}\nbody_bytes_used=${ranged.body_bytes_used}`;
    } else {
      workEl.textContent = "-";
      workEl.className = "v mono";
      workEl.title = "";
    }

    const ret = state.retention || {};
    $("retention-peak").textContent = fmt(ret.peak_bytes || 0);
    $("retention-split").textContent = `${fmt(ret.peak_memory_bytes || 0)} / ${fmt(ret.peak_disk_bytes || 0)}`;
    $("retention-used").textContent = `${fmt(ret.used_bytes || 0)} / ${fmt(ret.capacity_bytes || 0)}`;

    const sw = state.origin_switch || {};
    const swEl = $("origin-switch");
    if (sw.triggered) {
      swEl.hidden = false;
      const src = sw.source === "auto_on_pause" ? "慢端暂停时自动换版" : "手动换版";
      swEl.textContent = `${src}：源站 ${sw.from || "?"} → ${sw.to || "?"}；已开始的主下载仍按原版继续。`;
    } else {
      swEl.hidden = true;
    }

    renderEvents(state.events);

    if (state.error) showNotice(`演练出错：${state.error}`);
    else if (state.origin_error) showNotice(`源站状态读取失败：${state.origin_error}`, "info");
    else clearNotice();
  }

  async function poll() {
    try {
      const resp = await fetch("/api/drill");
      const state = await resp.json();
      render(state);
      if (state.status === "running") {
        pollTimer = setTimeout(poll, 300);
      } else {
        pollTimer = null;
        $("start-btn").disabled = false;
      }
    } catch (err) {
      showNotice(`无法读取状态：${err}`);
      pollTimer = setTimeout(poll, 1000);
    }
  }

  function startPolling() {
    if (pollTimer) clearTimeout(pollTimer);
    pollTimer = null;
    poll();
  }

  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    clearNotice();
    $("start-btn").disabled = true;
    try {
      const resp = await fetch("/api/drill", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(formPayload()),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        throw new Error(body.detail ? JSON.stringify(body.detail) : `HTTP ${resp.status}`);
      }
      startPolling();
    } catch (err) {
      $("start-btn").disabled = false;
      showNotice(`启动失败：${err.message || err}`);
    }
  });

  let currentOriginVersion = "v1";

  $("switch-btn").addEventListener("click", async () => {
    const target = currentOriginVersion === "v1" ? "v2" : "v1";
    try {
      const resp = await fetch(`/api/drill/switch?version=${target}`, { method: "POST" });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        throw new Error(body.detail ? JSON.stringify(body.detail) : `HTTP ${resp.status}`);
      }
      startPolling();
    } catch (err) {
      showNotice(`换版失败：${err.message || err}`);
    }
  });

  $("preset-128-btn").addEventListener("click", () => {
    // A one-click 128 MiB drill that is expected to finish both routes:
    // retain the whole 32 MiB tail (mostly spilled to disk) and run flat out.
    const set = (name, value) => {
      const input = form.querySelector(`[name="${name}"]`);
      if (input) input.value = String(value);
    };
    set("size_bytes", 128 * 1024 * 1024);
    set("chunk_size", 1024 * 1024);
    set("chunk_delay_ms", 0);
    set("retention_bytes", 33 * 1024 * 1024);
    set("retention_memory_bytes", 4 * 1024 * 1024);
    set("backfill_bytes", 0);
    set("pause_fraction", 0.75);
    const auto = form.querySelector('[name="auto_switch_on_pause"]');
    if (auto) auto.checked = false;
    clearNotice();
  });

  $("reset-btn").addEventListener("click", async () => {
    $("start-btn").disabled = false;
    try {
      const resp = await fetch("/api/drill/reset", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(formPayload()),
      });
      const body = await resp.json();
      render(body.state || {});
      if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
    } catch (err) {
      showNotice(`重置失败：${err.message || err}`);
    }
  });

  poll();
})();
