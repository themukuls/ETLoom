const runId = window.ETLOOM_RUN_ID;
const logPanel = document.getElementById("logs");
const statusBadge = document.getElementById("run-status");
const timeline = document.getElementById("timeline");

function statusClass(status) {
  const value = (status || "").toLowerCase();
  if (value === "success") return "success";
  if (value === "failed") return "error";
  return "running";
}

function renderRun(run) {
  statusBadge.textContent = run.status;
  statusBadge.className = `pill ${statusClass(run.status)}`;
  if (!run.steps || !run.steps.length) return;
  timeline.innerHTML = run.steps.map(step => `
    <div class="timeline-step ${statusClass(step.status)}">
      <span class="timeline-marker"></span>
      <div>
        <strong>${step.step_name}</strong>
        <span>${step.status}${step.duration_ms !== null ? ` · ${step.duration_ms} ms` : ""}</span>
        ${step.error_message ? `<p>${step.error_message}</p>` : ""}
      </div>
    </div>
  `).join("");
}

async function refreshRun() {
  const response = await fetch(`/api/runs/${runId}`);
  if (response.ok) renderRun(await response.json());
}

const events = new EventSource(`/api/runs/${runId}/events`);
events.onmessage = event => {
  const item = JSON.parse(event.data);
  const time = item.created_at ? new Date(item.created_at).toLocaleTimeString() : new Date().toLocaleTimeString();
  logPanel.textContent += `${time} ${item.level.padEnd(7)} ${item.message}\n`;
  logPanel.scrollTop = logPanel.scrollHeight;
  refreshRun();
};
events.addEventListener("complete", event => {
  renderRun(JSON.parse(event.data));
  events.close();
});
events.addEventListener("error", () => {
  if (events.readyState === EventSource.CLOSED) return;
  statusBadge.textContent = "Reconnecting";
});

document.getElementById("clear-logs").addEventListener("click", () => { logPanel.textContent = ""; });
refreshRun();
