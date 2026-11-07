const $ = (s) => document.querySelector(s);
const form = $("#job-form");
const taskType = $("#task-type");
const message = $("#form-message");
const recent = []; // job IDs submitted from this page, newest first
const jobs = new Map(); // job_id -> latest job JSON

function showFields() {
  document.querySelectorAll(".task-fields").forEach((el) => el.classList.add("hidden"));
  $(`#fields-${taskType.value}`)?.classList.remove("hidden");
}

function payload() {
  switch (taskType.value) {
    case "send_email": return { to: $("#to").value, subject: $("#subject").value };
    case "resize_image": return { width: Number($("#width").value), height: Number($("#height").value) };
    case "flaky_task": return { fail_rate: Number($("#fail-rate").value) };
    default: return {};
  }
}

async function submitJob() {
  const res = await fetch("/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ task_type: taskType.value, payload: payload() }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `Request failed (${res.status})`);
  recent.unshift(body.job_id);
  recent.length = Math.min(recent.length, 10);
  jobs.set(body.job_id, { job_id: body.job_id, task_type: body.task_type, status: body.status, attempts: 0 });
  return body;
}

async function send(count) {
  const buttons = form.querySelectorAll("button");
  buttons.forEach((b) => (b.disabled = true));
  message.className = "form-message";
  message.textContent = "Sending to the producer…";
  try {
    let last;
    for (let i = 0; i < count; i++) last = await submitJob();
    message.className = "form-message success";
    message.textContent = count === 1 ? `202 Accepted · ${last.job_id}` : `Queued ${count} jobs`;
    renderJobs();
    refresh();
  } catch (err) {
    message.className = "form-message error";
    message.textContent = err.message;
  } finally {
    buttons.forEach((b) => (b.disabled = false));
  }
}

function setText(id, v) { $(id).textContent = v; }

async function refreshStats() {
  try {
    const res = await fetch("/stats");
    if (!res.ok) throw new Error();
    const s = await res.json();
    setText("#m-queued", s.queued);
    setText("#m-inflight", s.in_flight);
    setText("#m-retrying", s.retrying);
    setText("#m-completed", s.completed);
    setText("#m-dead", s.dead_letter);
    setText("#m-workers", s.workers);
    setText("#flow-workers", `${s.workers} goroutines`);
    $(".service-status i").className = s.workers > 0 ? "" : "offline";
    setText("#service-text", s.workers > 0 ? "Workers online" : "No workers running");
  } catch {
    $(".service-status i").className = "offline";
    setText("#service-text", "Producer unreachable");
  }
}

async function refreshJobs() {
  await Promise.all(recent.map(async (id) => {
    const job = jobs.get(id);
    if (job && (job.status === "COMPLETED" || job.status === "FAILED")) return; // final
    const res = await fetch(`/jobs/${id}`).catch(() => null);
    if (res?.ok) jobs.set(id, await res.json());
  }));
  renderJobs();
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function renderJobs() {
  if (!recent.length) return;
  $("#jobs-body").innerHTML = recent.map((id) => {
    const j = jobs.get(id) || {};
    const max = j.max_attempts ? `/${j.max_attempts}` : "";
    const title = j.error_message ? ` title="${esc(j.error_message)}"` : "";
    return `<tr><td class="mono">${esc(id.slice(0, 8))}</td><td>${esc(j.task_type)}</td><td><span class="pill ${esc(j.status)}"${title}>${esc(j.status)}</span></td><td class="mono">${j.attempts ?? 0}${max}</td></tr>`;
  }).join("");
}

async function refreshDLQ() {
  const res = await fetch("/dlq?limit=8").catch(() => null);
  if (!res?.ok) return;
  const list = await res.json();
  $("#dlq-list").innerHTML = list.length
    ? list.map((d) => `<li><div><span class="mono">${esc(d.job.job_id.slice(0, 8))}</span> · ${esc(d.job.task_type)}<small>${esc(d.job.error_message)} · ${d.job.attempts} attempts</small></div><button class="replay" data-entry="${esc(d.entry_id)}">Replay</button></li>`).join("")
    : `<li class="empty">No dead letters. Submit a flaky task to see one land here.</li>`;
}

$("#dlq-list").addEventListener("click", async (e) => {
  const btn = e.target.closest(".replay");
  if (!btn) return;
  btn.disabled = true;
  const res = await fetch(`/dlq/${encodeURIComponent(btn.dataset.entry)}/replay`, { method: "POST" });
  if (res.ok) {
    const job = await res.json();
    if (!recent.includes(job.job_id)) recent.unshift(job.job_id);
    jobs.set(job.job_id, job);
  }
  refresh();
});

function refresh() { refreshStats(); refreshJobs(); refreshDLQ(); }

taskType.addEventListener("change", showFields);
$("#fail-rate").addEventListener("input", (e) => setText("#fail-rate-out", `${Math.round(e.target.value * 100)}%`));
form.addEventListener("submit", (e) => { e.preventDefault(); send(1); });
$("#burst").addEventListener("click", () => send(20));

showFields();
refresh();
setInterval(refresh, 1000);
