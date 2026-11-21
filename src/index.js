
import { spawn } from "node:child_process";

const scripts = [
  "src/server.js",
  "src/worker.js",
  "src/outbox-publisher.js",
];

const children = [];
const exited = new Set();

let shuttingDown = false;
let shutdownTimer;

function shutdown(exitCode = 0) {
  if (shuttingDown) return;

  shuttingDown = true;
  process.exitCode = exitCode;

  console.log("\nShutting down all application processes...");

  for (const child of children) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGTERM");
    }
  }

  shutdownTimer = setTimeout(() => {
    console.error("Graceful shutdown timed out. Forcing termination.");

    for (const child of children) {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
      }
    }
  }, 10000);

  shutdownTimer.unref();

  if (exited.size === children.length) {
    clearTimeout(shutdownTimer);
  }
}

for (const script of scripts) {
  console.log(`Starting ${script}...`);

  const child = spawn(process.execPath, [script], {
    stdio: "inherit",
    env: process.env,
  });

  children.push(child);

  child.on("error", (error) => {
    console.error(`Failed to start ${script}:`, error);
    shutdown(1);
  });

  child.on("exit", (code, signal) => {
    exited.add(child);

    console.log(
      `${script} exited with code ${code}, signal ${signal}`
    );

    if (!shuttingDown) {
      console.error(`${script} stopped unexpectedly.`);
      shutdown(code ?? 1);
    }

    if (shuttingDown && exited.size === children.length) {
      clearTimeout(shutdownTimer);
    }
  });
}

process.on("SIGTERM", () => shutdown(0));
process.on("SIGINT", () => shutdown(0));

console.log("Distributed Webhook Dispatcher started.");
