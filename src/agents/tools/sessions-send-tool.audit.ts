import crypto from "node:crypto";
import {
  emitTrustedMessageAuditEvent,
  hasTrustedMessageAuditListeners,
} from "../../audit/message-audit-events.js";

/** Minimal shape of what sessions_send actually returns, enough to classify delivery. */
type SessionsSendResultDetails = {
  status?: unknown;
  sessionKey?: unknown;
  runId?: unknown;
};

/**
 * Emits one terminal audit event for a sessions_send call, so "accepted"/"queued" at
 * the tool boundary is distinguishable in audit_events from genuine non-delivery.
 *
 * sessions_send has no platform/channel in the WhatsApp/Telegram sense -- the
 * "channel" here is the internal agent-to-agent delivery path itself.
 */
export function emitSessionsSendAuditEvent(params: {
  requesterAgentId: string;
  targetSessionKey?: string;
  result?: SessionsSendResultDetails;
  error?: unknown;
}): void {
  if (!hasTrustedMessageAuditListeners()) {
    return;
  }
  try {
    const status = typeof params.result?.status === "string" ? params.result.status : undefined;
    const targetId =
      params.targetSessionKey ??
      (typeof params.result?.sessionKey === "string" ? params.result.sessionKey : undefined);
    const runId = typeof params.result?.runId === "string" ? params.result.runId : undefined;
    // "ok"/"accepted"/"queued"/"no_reply"/"timeout" all mean the message reached the
    // target's queue or active run -- that is the delivery this tool can attest to.
    // sessions_send has no further platform hop, so reaching the target IS "sent".
    const delivered =
      !params.error &&
      (status === "ok" ||
        status === "accepted" ||
        status === "queued" ||
        status === "no_reply" ||
        status === "timeout");
    emitTrustedMessageAuditEvent({
      sourceId: `message:sessions-send:${runId ?? crypto.randomUUID()}`,
      occurredAt: Date.now(),
      kind: "message",
      direction: "outbound",
      actorType: "agent",
      actorId: params.requesterAgentId,
      agentId: params.requesterAgentId,
      ...(runId ? { runId } : {}),
      channel: "agent",
      conversationKind: "direct",
      ...(targetId ? { targetId } : {}),
      action: "message.outbound.finished",
      ...(delivered
        ? { status: "succeeded" as const, outcome: "sent" as const }
        : {
            status: "failed" as const,
            outcome: "failed" as const,
            errorCode: "message_delivery_failed" as const,
            failureStage: "queue" as const,
          }),
    });
  } catch {
    // Audit observers cannot alter delivery or queue semantics.
  }
}
