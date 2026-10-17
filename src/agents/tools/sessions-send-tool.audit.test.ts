import { describe, expect, it } from "vitest";
import { onTrustedMessageAuditEvent } from "../../audit/message-audit-events.js";
import { emitSessionsSendAuditEvent } from "./sessions-send-tool.audit.js";

function captureOne() {
  const events: unknown[] = [];
  const unsubscribe = onTrustedMessageAuditEvent((event) => events.push(event));
  return { events, unsubscribe };
}

describe("emitSessionsSendAuditEvent", () => {
  it("records a successful delivery for an accepted/steered result", () => {
    const { events, unsubscribe } = captureOne();
    try {
      emitSessionsSendAuditEvent({
        requesterAgentId: "main",
        result: { status: "accepted", sessionKey: "agent:work:main", runId: "run-1" },
      });
    } finally {
      unsubscribe();
    }
    expect(events).toEqual([
      expect.objectContaining({
        kind: "message",
        direction: "outbound",
        actorType: "agent",
        actorId: "main",
        agentId: "main",
        runId: "run-1",
        channel: "agent",
        conversationKind: "direct",
        targetId: "agent:work:main",
        action: "message.outbound.finished",
        status: "succeeded",
        outcome: "sent",
      }),
    ]);
  });

  it("records a successful delivery for a no_reply/timeout result (message still reached the target)", () => {
    const { events, unsubscribe } = captureOne();
    try {
      emitSessionsSendAuditEvent({
        requesterAgentId: "main",
        result: { status: "no_reply", sessionKey: "agent:work:main", runId: "run-2" },
      });
      emitSessionsSendAuditEvent({
        requesterAgentId: "main",
        result: { status: "timeout", sessionKey: "agent:work:main", runId: "run-3" },
      });
    } finally {
      unsubscribe();
    }
    expect(events).toHaveLength(2);
    for (const event of events) {
      expect(event).toMatchObject({ status: "succeeded", outcome: "sent" });
    }
  });

  it("records a failed delivery for an error/forbidden result", () => {
    const { events, unsubscribe } = captureOne();
    try {
      emitSessionsSendAuditEvent({
        requesterAgentId: "main",
        result: { status: "forbidden", sessionKey: "agent:work:main", runId: "run-4" },
      });
    } finally {
      unsubscribe();
    }
    expect(events).toEqual([
      expect.objectContaining({
        status: "failed",
        outcome: "failed",
        errorCode: "message_delivery_failed",
        failureStage: "queue",
        targetId: "agent:work:main",
      }),
    ]);
  });

  it("records a failed delivery when the tool call throws", () => {
    const { events, unsubscribe } = captureOne();
    try {
      emitSessionsSendAuditEvent({
        requesterAgentId: "main",
        error: new Error("boom"),
      });
    } finally {
      unsubscribe();
    }
    expect(events).toEqual([expect.objectContaining({ status: "failed", outcome: "failed" })]);
  });

  it("does nothing when there are no audit listeners", () => {
    // No listener registered; must not throw.
    expect(() =>
      emitSessionsSendAuditEvent({
        requesterAgentId: "main",
        result: { status: "ok", sessionKey: "agent:work:main", runId: "run-5" },
      }),
    ).not.toThrow();
  });
});
