import { z } from "zod";

export const webhookSchema = z.object({
  destinationUrl: z.string().url(),
  eventId: z.string().min(1),
  payload: z.record(z.string(), z.unknown()),
});