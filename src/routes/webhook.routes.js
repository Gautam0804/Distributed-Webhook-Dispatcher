import { Router } from "express";
import { dispatchWebhook } from "../controllers/webhook.controller.js";

const router = Router();

router.post("/dispatch", dispatchWebhook);

export default router;