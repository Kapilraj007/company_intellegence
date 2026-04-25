import { T } from "../theme";
import { Btn, Card } from "../components";
import { Logo } from "../components";

export default function ApprovalPendingPage({ signupInfo, onBackToLogin }) {
  const name = signupInfo?.name || "Your account";
  const email = signupInfo?.email || "";

  return (
    <div style={{
      minHeight: "100vh",
      background: T.navy,
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      position: "relative",
      overflow: "hidden",
      padding: "24px 16px",
    }}>
      <div style={{
        position: "absolute", inset: 0, opacity: 0.04,
        backgroundImage: `linear-gradient(${T.cyan} 1px, transparent 1px), linear-gradient(90deg, ${T.cyan} 1px, transparent 1px)`,
        backgroundSize: "60px 60px",
      }}/>
      <div style={{ position: "absolute", top: "18%", left: "12%", width: 280, height: 280, borderRadius: "50%", background: T.amber + "08", filter: "blur(80px)" }}/>
      <div style={{ position: "absolute", bottom: "16%", right: "14%", width: 240, height: 240, borderRadius: "50%", background: T.cyan + "08", filter: "blur(80px)" }}/>

      <div style={{ width: "100%", maxWidth: 460 }}>
        <div style={{ textAlign: "center", marginBottom: 28 }}>
          <Logo size={26} />
          <p style={{ marginTop: 12, fontSize: 14, color: T.muted }}>Company Intelligence Platform</p>
        </div>

        <Card style={{ padding: 36 }}>
          <div style={{
            width: 62,
            height: 62,
            borderRadius: 16,
            background: `linear-gradient(135deg, ${T.amber}2A, ${T.cyan}22)`,
            border: `1px solid ${T.amber}35`,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: 28,
            marginBottom: 18,
          }}>
            ⏳
          </div>

          <h2 style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: 24, color: T.white, marginBottom: 10 }}>
            Approval Pending
          </h2>
          <p style={{ color: T.muted, fontSize: 14, lineHeight: 1.7 }}>
            Request has been sent to admin for approval. We’ll unlock the workspace for <span style={{ color: T.white }}>{name}</span> as soon as access is approved.
          </p>

          <div style={{
            marginTop: 20,
            background: T.navy3,
            border: `1px solid ${T.border}`,
            borderRadius: 10,
            padding: "14px 16px",
            color: T.white,
            fontSize: 13,
          }}>
            <div style={{ color: T.muted, fontSize: 11, marginBottom: 6 }}>Requested account</div>
            <div>{email || "Pending email confirmation"}</div>
          </div>

          <div style={{
            marginTop: 18,
            background: T.cyan + "10",
            border: `1px solid ${T.cyan}25`,
            borderRadius: 10,
            padding: "12px 14px",
            color: T.cyan,
            fontSize: 13,
            lineHeight: 1.6,
          }}>
            You can’t access pipeline, search, or analytics features until an admin marks the account as approved.
          </div>

          <Btn onClick={onBackToLogin} full style={{ marginTop: 22, padding: "13px 22px", fontSize: 14 }}>
            Back to Sign In
          </Btn>
        </Card>
      </div>
    </div>
  );
}
