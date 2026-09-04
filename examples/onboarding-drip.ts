/**
 * The canonical "why would I want this" example: a user-onboarding drip
 * sequence. Nothing here is Stepline-specific business logic — email sending
 * is stubbed as a console.log — the point is the shape: send, wait, send,
 * wait, branch, and the whole thing survives a crash at any point.
 *
 * Delays are read from input so the demo (examples/run.ts) can compress
 * "1 day" into a couple of seconds; a real deployment would pass real
 * day-scale numbers (or just hard-code them and drop the input field).
 */
import { defineWorkflow, once, type StepContext } from "../src/index.js";
import type { Pool } from "pg";

export interface OnboardingInput {
  userEmail: string;
  /** Simulated: does the user do the "activated" action before the nudge fires? */
  willActivate: boolean;
  /** Compressed delays for the demo. Defaults to real day-scale if omitted. */
  demo?: { day1Ms: number; day3Ms: number };
}

async function sendEmail(kind: string, to: string): Promise<{ sentAt: string }> {
  // Stand-in for calling a real provider (Postfmark, SES, whatever).
  console.log(`  [email] ${kind} -> ${to}`);
  return { sentAt: new Date().toISOString() };
}

export function registerOnboardingDrip(pool: Pool) {
  defineWorkflow<OnboardingInput>({
    type: "onboarding-drip",
    start: "sendWelcome",
    steps: {
      sendWelcome: {
        retry: { max: 3 },
        async run(ctx: StepContext<OnboardingInput>) {
          const result = await once(pool, ctx.idempotencyKey, () =>
            sendEmail("welcome", ctx.input.userEmail),
          );
          return { type: "continue", next: "waitOneDay", output: result };
        },
      },
      waitOneDay: {
        async run(ctx: StepContext<OnboardingInput>) {
          const delayMs = ctx.input.demo?.day1Ms ?? 24 * 60 * 60 * 1000;
          return { type: "wait", next: "sendTips", delayMs };
        },
      },
      sendTips: {
        retry: { max: 3 },
        async run(ctx: StepContext<OnboardingInput>) {
          const result = await once(pool, ctx.idempotencyKey, () =>
            sendEmail("tips", ctx.input.userEmail),
          );
          return { type: "continue", next: "waitThreeDays", output: result };
        },
      },
      waitThreeDays: {
        async run(ctx: StepContext<OnboardingInput>) {
          const delayMs = ctx.input.demo?.day3Ms ?? 3 * 24 * 60 * 60 * 1000;
          return { type: "wait", next: "checkActivation", delayMs };
        },
      },
      checkActivation: {
        async run(ctx: StepContext<OnboardingInput>) {
          if (ctx.input.willActivate) {
            return { type: "complete", output: { activated: true } };
          }
          return { type: "continue", next: "sendNudge" };
        },
      },
      sendNudge: {
        retry: { max: 3 },
        async run(ctx: StepContext<OnboardingInput>) {
          const result = await once(pool, ctx.idempotencyKey, () =>
            sendEmail("nudge", ctx.input.userEmail),
          );
          return { type: "complete", output: result };
        },
      },
    },
  });
}
