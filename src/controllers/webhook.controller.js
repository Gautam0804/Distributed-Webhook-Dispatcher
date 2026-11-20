
import { webhookSchema } from "../schemas/webhook.schema.js";
import { enqueueWebhook } from "../services/webhook.service.js";
import { validateDestinationUrl } from "../services/url-security.service.js";

export async function dispatchWebhook(req, res, next) {
  try {
    // 1. Validate request payload.
    const result = webhookSchema.safeParse(req.body);

    if (!result.success) {
      return res.status(400).json({
        success: false,
        message: "Invalid webhook payload",
        errors: result.error.flatten(),
      });
    }

    // 2. Validate destination URL against SSRF.
    try {
      await validateDestinationUrl(result.data.destinationUrl);
    } catch (error) {
      return res.status(400).json({
        success: false,
        message: "Unsafe destination URL",
        error: error.message,
      });
    }

    // 3. Enqueue only after both validations pass.
    const job = await enqueueWebhook(result.data);

    // 4. Return 202 Accepted.
    return res.status(202).json({
      success: true,
      message: "Webhook accepted and saved to outbox",
      ...job,
    });

  } catch (error) {
    next(error);
  }
}
