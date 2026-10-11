// Failure alerts must describe only cron outcomes that survived durable persistence.
import { describe, expect, it, vi } from "vitest";
import { setupCronRegressionFixtures } from "../../../test/helpers/cron/service-regression-fixtures.js";
import { openOpenClawStateDatabase } from "../../state/openclaw-state-db.js";
import { cronScriptFailureMetadata } from "../script-failure.js";
import { loadCronStore, saveCronStore } from "../store.js";
import { cronStoreKey } from "../store/key.js";
import type { CronJob } from "../types.js";
import {
  createAlertJob,
  createAlertState,
  finalizeAlertOutcome,
  type SendCronFailureAlert,
} from "./failure-alerts.test-support.js";
import {
  applyJobResultAndDrainNotifications,
  applyTriggerNoFireResultAndDrainNotifications,
} from "./notification.test-helpers.js";
import { restoreFinalizedStartupRun } from "./startup-run-repair.js";
import type { DeferredCronNotifications } from "./state.js";

const fixtures = setupCronRegressionFixtures({
  prefix: "cron-failure-alert-persistence-",
});

describe("cron failure alert persistence", () => {
  it("shares cooldown across alternating execution and delivery failures", () => {
    const store = fixtures.makeStorePath();
    let now = Date.parse("2026-08-01T14:49:00Z");
    const job = createAlertJob({ id: "shared-alert-cooldown", dueAt: now });
    job.delivery = {
      mode: "announce",
      failureDestination: { mode: "webhook", to: "https://alerts.example.test/cron" },
    };
    const sendCronFailureAlert = vi.fn<SendCronFailureAlert>(async () => undefined);
    const state = createAlertState({
      storePath: store.storePath,
      nowMs: () => now,
      sendCronFailureAlert,
    });
    for (const status of ["error", "ok"] as const) {
      applyJobResultAndDrainNotifications(state, job, {
        status,
        delivered: false,
        error: status === "error" ? "execution failed" : undefined,
        deliveryError: "primary rejected",
        startedAt: now,
        endedAt: now,
      });
      now += 1_000;
    }
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    applyJobResultAndDrainNotifications(state, job, {
      status: "ok",
      delivered: true,
      startedAt: now,
      endedAt: now,
    });
    expect(job.state.lastFailureAlertAtMs).toBeUndefined();
    expect(job.state.failureAlertIncident).toBeUndefined();
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    applyJobResultAndDrainNotifications(state, job, {
      status: "ok",
      delivered: false,
      deliveryError: "primary rejected",
      startedAt: now,
      endedAt: now,
    });
    expect(sendCronFailureAlert).toHaveBeenCalledTimes(2);
  });

  it.each(["skipped run", "quiet trigger"])(
    "keeps delivery cooldown across a %s",
    (intervening) => {
      const store = fixtures.makeStorePath();
      const firstAt = Date.parse("2026-08-01T14:49:00Z");
      let now = firstAt;
      const job = createAlertJob({ id: "delivery-alert-order", dueAt: now });
      job.delivery = {
        mode: "announce",
        failureDestination: { mode: "webhook", to: "https://alerts.example.test/cron" },
      };
      const sendCronFailureAlert = vi.fn(async () => undefined);
      const state = createAlertState({
        storePath: store.storePath,
        nowMs: () => now,
        sendCronFailureAlert,
      });
      const failDelivery = () =>
        applyJobResultAndDrainNotifications(state, job, {
          status: "ok",
          delivered: false,
          deliveryError: "primary rejected",
          startedAt: now,
          endedAt: now,
        });
      failDelivery();
      expect(sendCronFailureAlert).toHaveBeenCalledOnce();
      now += 1_000;
      if (intervening === "skipped run") {
        applyJobResultAndDrainNotifications(state, job, {
          status: "skipped",
          startedAt: now,
          endedAt: now,
        });
      } else {
        applyTriggerNoFireResultAndDrainNotifications(state, job, {
          startedAt: now,
          endedAt: now,
          triggerEval: { fired: false, stateChanged: false },
        });
      }
      now += 1_000;
      failDelivery();
      expect(sendCronFailureAlert).toHaveBeenCalledOnce();
      expect(job.state.lastFailureAlertAtMs).toBe(firstAt);
    },
  );

  it.each([
    ["recorded error", { status: "unknown" }, -20_000, 0, "error", true],
    ["recorded intent with alerts disabled", { status: "unknown" }, -20_000, 0, "ok", false],
    ["newer alert", { status: "delivered", delivered: true }, 10_000, 10_000, "error", false],
  ] as const)(
    "restores cooldown from %s without transport",
    (_name, notification, priorOffset, expectedOffset, status, enabled) => {
      const endedAt = Date.parse("2026-08-01T14:50:00Z");
      const store = fixtures.makeStorePath();
      const job = createAlertJob({ id: "delivery-replay", dueAt: endedAt - 10 });
      job.delivery = { mode: "none" };
      job.failureAlert = enabled ? { after: 1, cooldownMs: 60_000, includeSkipped: true } : false;
      job.state.lastFailureAlertAtMs = endedAt + priorOffset;
      const sendCronFailureAlert = vi.fn(async () => undefined);
      const state = createAlertState({
        storePath: store.storePath,
        nowMs: () => endedAt + 30_000,
        sendCronFailureAlert,
      });
      const deferredNotifications: DeferredCronNotifications = [];
      restoreFinalizedStartupRun({
        state,
        job,
        runningAtMs: endedAt - 10,
        deferredNotifications,
        entry: {
          ts: endedAt,
          jobId: job.id,
          action: "finished",
          status,
          completionStatus: "failed",
          deliveryStatus: "not-delivered",
          deliveryError: "primary rejected",
          failureNotificationDelivery: notification,
          runAtMs: endedAt - 10,
        },
      });
      expect(job.state.lastFailureAlertAtMs).toBe(endedAt + expectedOffset);
      expect(job.state.lastFailureNotificationDeliveryStatus).toBe(
        notification?.status ?? "not-requested",
      );
      expect(deferredNotifications).toEqual([]);
      expect(sendCronFailureAlert).not.toHaveBeenCalled();
      expect(state.deps.enqueueSystemEvent).not.toHaveBeenCalled();
    },
  );

  it("resumes skipped alerts after a clock rollback only after durable persistence", async () => {
    const store = fixtures.makeStorePath();
    const dueAt = Date.parse("2026-08-01T14:52:00.000Z");
    const job = createAlertJob({
      id: "skipped-alert-clock-rollback",
      dueAt,
      includeSkipped: true,
    });
    job.state.lastFailureAlertAtMs = dueAt + 3_600_000;
    await saveCronStore(store.storePath, { version: 1, jobs: [job] });

    let now = dueAt;
    let durableAtSend: CronJob["state"] | undefined;
    const sendCronFailureAlert = vi.fn(async () => {
      durableAtSend = (await loadCronStore(store.storePath)).jobs[0]?.state;
    });
    const state = createAlertState({
      storePath: store.storePath,
      nowMs: () => now,
      sendCronFailureAlert,
    });

    await finalizeAlertOutcome({
      state,
      job,
      status: "skipped",
      error: "provider unavailable",
      startedAt: now,
      endedAt: now + 10,
    });

    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    await sendCronFailureAlert.mock.results[0]?.value;
    expect(durableAtSend).toMatchObject({
      lastFailureAlertAtMs: dueAt,
      lastFailureNotificationDeliveryStatus: "unknown",
    });
    expect((await loadCronStore(store.storePath)).jobs[0]?.state.lastFailureAlertAtMs).toBe(now);

    now += 30_000;
    const currentJob = state.store?.jobs[0];
    if (!currentJob) {
      throw new Error("expected persisted cron job");
    }
    await finalizeAlertOutcome({
      state,
      job: currentJob,
      status: "skipped",
      error: "provider still unavailable",
      startedAt: now,
      endedAt: now + 10,
    });

    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    expect((await loadCronStore(store.storePath)).jobs[0]?.state.lastFailureAlertAtMs).toBe(dueAt);
  });

  it("rolls back the cooldown without delivery when persistence fails", async () => {
    const store = fixtures.makeStorePath();
    const dueAt = Date.parse("2026-08-01T14:55:00.000Z");
    const job = createAlertJob({ id: "failure-alert-persist-rollback", dueAt });
    await saveCronStore(store.storePath, { version: 1, jobs: [job] });

    const sendCronFailureAlert = vi.fn(async () => undefined);
    const state = createAlertState({
      storePath: store.storePath,
      nowMs: () => dueAt + 10,
      sendCronFailureAlert,
    });
    const database = openOpenClawStateDatabase().db;
    database.exec(`
      CREATE TRIGGER reject_failure_alert_terminal_write
      BEFORE UPDATE ON cron_jobs
      WHEN NEW.store_key = '${cronStoreKey(store.storePath)}' AND NEW.job_id = '${job.id}'
      BEGIN
        SELECT RAISE(ABORT, 'terminal write failed');
      END;
    `);

    try {
      await expect(
        finalizeAlertOutcome({
          state,
          job,
          status: "error",
          error: "provider unavailable",
          startedAt: dueAt,
          endedAt: dueAt + 10,
        }),
      ).rejects.toThrow("terminal write failed");

      expect(sendCronFailureAlert).not.toHaveBeenCalled();
      expect(state.store?.jobs[0]?.state.lastFailureAlertAtMs).toBeUndefined();
      expect(
        (await loadCronStore(store.storePath)).jobs[0]?.state.lastFailureAlertAtMs,
      ).toBeUndefined();
    } finally {
      database.exec("DROP TRIGGER IF EXISTS reject_failure_alert_terminal_write");
    }
  });

  it("persists an incident and suppresses repeats after a service reload past cooldown", async () => {
    const store = fixtures.makeStorePath();
    const dueAt = Date.parse("2026-08-01T14:58:00.000Z");
    const firstAlertAt = dueAt + 10;
    const job = createAlertJob({ id: "failure-alert-cooldown-persisted", dueAt });
    await saveCronStore(store.storePath, { version: 1, jobs: [job] });

    let now = firstAlertAt;
    const sendCronFailureAlert = vi.fn(async () => undefined);
    const state = createAlertState({
      storePath: store.storePath,
      nowMs: () => now,
      sendCronFailureAlert,
    });

    await finalizeAlertOutcome({
      state,
      job,
      status: "error",
      error: "first failure",
      startedAt: dueAt,
      endedAt: firstAlertAt,
    });
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    expect((await loadCronStore(store.storePath)).jobs[0]?.state).toMatchObject({
      lastFailureAlertAtMs: firstAlertAt,
      lastFailureNotificationDeliveryStatus: "unknown",
    });

    now += 30_000;
    const currentJob = state.store?.jobs[0];
    if (!currentJob) {
      throw new Error("expected persisted cron job");
    }
    await finalizeAlertOutcome({
      state,
      job: currentJob,
      status: "error",
      error: "first failure",
      startedAt: now,
      endedAt: now + 10,
    });

    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    // Regression: this run did not request its own alert (dedup/cooldown
    // suppressed it), so the prior run's recorded delivery fact
    // ("unknown", not yet settled in this test) must be preserved rather
    // than clobbered back to "not-requested" — see timer-outcomes.ts.
    expect((await loadCronStore(store.storePath)).jobs[0]).toMatchObject({
      state: {
        consecutiveErrors: 2,
        lastFailureAlertAtMs: firstAlertAt,
        lastFailureNotificationDeliveryStatus: "unknown",
      },
    });

    now += 60_000;
    const reloadedState = createAlertState({
      storePath: store.storePath,
      nowMs: () => now,
      sendCronFailureAlert,
    });
    await finalizeAlertOutcome({
      state: reloadedState,
      job: currentJob,
      status: "error",
      error: "first failure",
      startedAt: now,
      endedAt: now + 10,
    });
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();

    const recoveredJob = reloadedState.store?.jobs[0];
    if (!recoveredJob) {
      throw new Error("expected reloaded cron job");
    }
    now += 1_000;
    await finalizeAlertOutcome({
      state: reloadedState,
      job: recoveredJob,
      status: "ok",
      startedAt: now,
      endedAt: now + 10,
    });
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    const finalState = (await loadCronStore(store.storePath)).jobs[0]?.state;
    // toMatchObject treats an explicit `undefined` expectation as requiring
    // the key to be present-but-undefined, not merely absent — assert this
    // field separately rather than folding it into the toMatchObject below.
    expect(finalState?.failureAlertIncident).toBeUndefined();
    // A run that actually resolves the failure chain (completionStatus
    // succeeded) is the one case that clears the prior delivery fact.
    expect(finalState).toMatchObject({
      lastFailureNotificationDeliveryStatus: "not-requested",
    });
  });

  it("preserves a prior delivered-or-unknown outcome across a second failure suppressed by cooldown alone", async () => {
    const store = fixtures.makeStorePath();
    const dueAt = Date.parse("2026-08-02T09:00:00.000Z");
    const firstAlertAt = dueAt + 10;
    const job = createAlertJob({ id: "cooldown-only-suppression", dueAt });
    await saveCronStore(store.storePath, { version: 1, jobs: [job] });

    let now = firstAlertAt;
    const sendCronFailureAlert = vi.fn<SendCronFailureAlert>(async () => undefined);
    const state = createAlertState({
      storePath: store.storePath,
      nowMs: () => now,
      sendCronFailureAlert,
    });

    await finalizeAlertOutcome({
      state,
      job,
      status: "error",
      error: "disk pressure on backup volume",
      startedAt: dueAt,
      endedAt: firstAlertAt,
    });
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    expect((await loadCronStore(store.storePath)).jobs[0]?.state).toMatchObject({
      lastFailureAlertAtMs: firstAlertAt,
      lastFailureNotificationDeliveryStatus: "unknown",
    });

    // A second, genuinely different failure inside the 60s cooldown window.
    // Different error text rules out incident-signature dedup as the cause
    // of suppression here — only the cooldown should be suppressing this one.
    now += 30_000;
    const currentJob = state.store?.jobs[0];
    if (!currentJob) {
      throw new Error("expected persisted cron job");
    }
    await finalizeAlertOutcome({
      state,
      job: currentJob,
      status: "error",
      error: "network timeout contacting backup target",
      startedAt: now,
      endedAt: now + 10,
    });

    // Regression for the core bug: no second send happened (still suppressed,
    // this time purely by cooldown), and the job's lastFailureAlertAtMs did
    // not advance — so the first alert's recorded outcome must still be the
    // value read back, not reset to "not-requested" by this run's own
    // finalization.
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    expect((await loadCronStore(store.storePath)).jobs[0]?.state).toMatchObject({
      consecutiveErrors: 2,
      lastFailureAlertAtMs: firstAlertAt,
      lastFailureNotificationDeliveryStatus: "unknown",
    });
  });

  it("alerts once after a timeout error at after:1, then clears failure-notification state on the next successful run", async () => {
    const store = fixtures.makeStorePath();
    const dueAt = Date.parse("2026-08-03T10:00:00.000Z");
    const job = createAlertJob({ id: "timeout-after-one-then-recovers", dueAt });
    await saveCronStore(store.storePath, { version: 1, jobs: [job] });

    let now = dueAt + 10;
    const sendCronFailureAlert = vi.fn<SendCronFailureAlert>(async () => undefined);
    const state = createAlertState({
      storePath: store.storePath,
      nowMs: () => now,
      sendCronFailureAlert,
    });

    // A single timeout error with after:1 must alert on the very first failure.
    await finalizeAlertOutcome({
      state,
      job,
      status: "error",
      error: "cron: job execution timed out (last phase: tool-execution-started)",
      startedAt: dueAt,
      endedAt: now,
    });
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    expect((await loadCronStore(store.storePath)).jobs[0]?.state).toMatchObject({
      lastFailureAlertAtMs: now,
      lastFailureNotificationDeliveryStatus: "unknown",
    });

    now += 300_000;
    const recoveredJob = state.store?.jobs[0];
    if (!recoveredJob) {
      throw new Error("expected persisted cron job");
    }
    await finalizeAlertOutcome({
      state,
      job: recoveredJob,
      status: "ok",
      startedAt: now,
      endedAt: now + 10,
    });

    // A successful run is the one case that clears the failure-notification
    // fields, independent of cooldown — this is the "resolves the chain" path.
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    const recoveredState = (await loadCronStore(store.storePath)).jobs[0]?.state;
    expect(recoveredState?.lastFailureAlertAtMs).toBeUndefined();
    expect(recoveredState).toMatchObject({
      lastFailureNotificationDeliveryStatus: "not-requested",
    });
  });

  it("does not misclassify a run as failed, and never touches failure-notification state, when the run ultimately succeeds", async () => {
    // Boundary case for this fix: proving the failure-notification
    // preservation change is scoped to bookkeeping fields only and does not
    // broaden what counts as a failed *run* — an ordinary successful
    // completion (standing in for "an intermediate tool error that the agent
    // recovered from before finishing") must never request or record a
    // failure alert at all.
    const store = fixtures.makeStorePath();
    const now = Date.parse("2026-08-04T11:00:00.000Z");
    const job = createAlertJob({ id: "ordinary-success-no-alert", dueAt: now });
    await saveCronStore(store.storePath, { version: 1, jobs: [job] });

    const sendCronFailureAlert = vi.fn<SendCronFailureAlert>(async () => undefined);
    const state = createAlertState({
      storePath: store.storePath,
      nowMs: () => now,
      sendCronFailureAlert,
    });

    await finalizeAlertOutcome({
      state,
      job,
      status: "ok",
      startedAt: now,
      endedAt: now + 10,
    });

    expect(sendCronFailureAlert).not.toHaveBeenCalled();
    const ordinaryState = (await loadCronStore(store.storePath)).jobs[0]?.state;
    expect(ordinaryState?.lastFailureAlertAtMs).toBeUndefined();
    expect(ordinaryState).toMatchObject({
      consecutiveErrors: 0,
      lastFailureNotificationDeliveryStatus: "not-requested",
    });
  });

  it("keeps a trigger incident unresolved while its quiet evaluation is busy", () => {
    const store = fixtures.makeStorePath();
    const now = Date.parse("2026-08-01T15:00:00Z");
    const job = createAlertJob({ id: "trigger-recovery-scope", dueAt: now });
    const sendCronFailureAlert = vi.fn<SendCronFailureAlert>(async () => undefined);
    const state = createAlertState({
      storePath: store.storePath,
      nowMs: () => now,
      sendCronFailureAlert,
    });
    applyJobResultAndDrainNotifications(state, job, {
      status: "error",
      error: "plugin reload failed",
      failureNotificationDetail: {
        kind: "script-failure",
        source: "trigger",
        code: "plugin_reload_failed",
      },
      startedAt: now,
      endedAt: now,
    });
    applyTriggerNoFireResultAndDrainNotifications(state, job, {
      startedAt: now + 1_000,
      endedAt: now + 1_001,
      triggerEval: { fired: false, stateChanged: false, busy: true },
    });
    // Recovery is silent; only the original failure alert is ever sent.
    expect(sendCronFailureAlert).toHaveBeenCalledOnce();
    expect(job.state.failureAlertIncident).toBeDefined();
  });

  it.each([
    { after: 1, triggerFirst: true },
    { after: 3, triggerFirst: true },
  ])(
    "keeps a payload failure open across trigger checks (threshold=$after, triggerFirst=$triggerFirst)",
    ({ after, triggerFirst }) => {
      const store = fixtures.makeStorePath();
      let now = Date.parse("2026-08-01T15:00:00Z");
      const job = createAlertJob({ id: "mixed-trigger-payload-incident", dueAt: now });
      const sendCronFailureAlert = vi.fn<SendCronFailureAlert>(async () => undefined);
      const state = createAlertState({
        storePath: store.storePath,
        nowMs: () => now,
        sendCronFailureAlert,
      });
      const fail = (source: "trigger" | "payload", code: "timeout" | "plugin_reload_failed") =>
        applyJobResultAndDrainNotifications(state, job, {
          status: "error",
          error: "script failure",
          ...cronScriptFailureMetadata(source, code),
          startedAt: now,
          endedAt: now,
        });
      if (triggerFirst) {
        fail("trigger", "timeout");
      }
      job.failureAlert = { after, cooldownMs: 60_000 };
      now += 1_000;
      fail("payload", "timeout");
      expect(sendCronFailureAlert).toHaveBeenCalledTimes(triggerFirst ? 1 : 0);
      now += 60_000;
      fail("trigger", "plugin_reload_failed");
      const incidentAlerts = triggerFirst ? 2 : 1;
      expect(sendCronFailureAlert).toHaveBeenCalledTimes(incidentAlerts);
      applyTriggerNoFireResultAndDrainNotifications(state, job, {
        startedAt: now + 1_000,
        endedAt: now + 1_001,
        triggerEval: { fired: false, stateChanged: false },
      });
      expect(sendCronFailureAlert).toHaveBeenCalledTimes(incidentAlerts);
      expect(job.state.failureAlertIncident?.scope).toBe("run");
      applyJobResultAndDrainNotifications(state, job, {
        status: "ok",
        startedAt: now + 2_000,
        endedAt: now + 2_001,
      });
      expect(sendCronFailureAlert).toHaveBeenCalledTimes(incidentAlerts);
      expect(job.state.failureAlertIncident).toBeUndefined();
    },
  );
});
