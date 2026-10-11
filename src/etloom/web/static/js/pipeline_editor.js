const editor = document.getElementById("yaml-editor");
const message = document.getElementById("message");
const state = document.getElementById("editor-state");

function showMessage(text, kind) {
  message.textContent = text;
  message.className = `message ${kind}`;
}

editor.addEventListener("input", () => {
  state.textContent = "Unsaved changes";
  state.className = "pill warning";
});

document.getElementById("validate-button").addEventListener("click", async () => {
  const response = await fetch("/api/pipelines/validate", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({yaml_content: editor.value})
  });
  const result = await response.json();
  showMessage(result.valid ? result.message : result.errors, result.valid ? "success" : "error");
});

document.getElementById("save-button").addEventListener("click", async () => {
  const response = await fetch(`/api/pipelines/${window.ETLOOM_PIPELINE_ID}`, {
    method: "PUT",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({yaml_content: editor.value})
  });
  const result = await response.json();
  if (!response.ok) {
    showMessage(result.detail || "Could not save configuration", "error");
    return;
  }
  state.textContent = "Saved";
  state.className = "pill success";
  showMessage("Pipeline configuration validated and saved.", "success");
});

document.getElementById("run-button").addEventListener("click", async () => {
  showMessage("Starting pipeline…", "info");
  const response = await fetch(`/api/pipelines/${window.ETLOOM_PIPELINE_ID}/runs`, {method: "POST"});
  const result = await response.json();
  if (!response.ok) {
    showMessage(result.detail || "Could not start pipeline", "error");
    return;
  }
  window.location.href = `/runs/${result.run_id}`;
});
