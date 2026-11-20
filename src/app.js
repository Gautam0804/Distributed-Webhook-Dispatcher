import express from "express";
import webhookRoutes from "./routes/webhook.routes.js";

const app = express();

app.use(express.json({ limit: "1mb" }));

app.get("/health", (req, res) => {
  res.status(200).json({
    success: true,
    message: "Webhook Dispatcher API is running",
  });
});

app.use("/api/webhooks", webhookRoutes);

app.use((err, req, res, next) => {
  console.error("Error:", err);

  res.status(500).json({
    success: false,
    message: "Internal server error",
  });
});

export default app;