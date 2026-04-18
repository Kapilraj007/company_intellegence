import { T } from "./theme";

export const Logo = ({ size = 22 }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
    <svg width={size + 4} height={size + 4} viewBox="0 0 28 28" fill="none">
      <polygon points="14,2 26,8 26,20 14,26 2,20 2,8" stroke={T.amber} strokeWidth="1.5" fill="none"/>
      <polygon points="14,7 21,11 21,17 14,21 7,17 7,11" fill={T.amber} opacity="0.2"/>
      <circle cx="14" cy="14" r="3" fill={T.amber}/>
    </svg>
    <span style={{ fontFamily: "'Syne', sans-serif", fontWeight: 800, fontSize: size, letterSpacing: "-0.5px", color: T.white }}>
      INTEL<span style={{ color: T.amber }}>IQ</span>
    </span>
  </div>
);

export const Badge = ({ children, color = T.amber }) => (
  <span style={{
    display: "inline-flex", alignItems: "center", gap: 5, flexWrap: "wrap", maxWidth: "100%",
    background: color + "18", border: `1px solid ${color}40`,
    color, borderRadius: 4, padding: "2px 9px",
    fontSize: 11, fontFamily: "'JetBrains Mono', monospace", fontWeight: 500,
    letterSpacing: "0.5px", textTransform: "uppercase",
    whiteSpace: "normal", wordBreak: "break-word",
  }}>{children}</span>
);

export const Dot = ({ color, pulse }) => (
  <span style={{
    display: "inline-block", width: 7, height: 7, borderRadius: "50%",
    background: color, flexShrink: 0,
    animation: pulse ? "pulse 1.5s ease-in-out infinite" : "none",
    boxShadow: `0 0 6px ${color}`,
  }}/>
);

export const Card = ({ children, style, className }) => (
  <div className={className} style={{
    background: T.navy2, border: `1px solid ${T.border}`,
    borderRadius: 12, ...style,
  }}>{children}</div>
);

export const Btn = ({ children, onClick, variant = "primary", disabled, style, full, ...props }) => {
  const base = {
    display: "inline-flex", alignItems: "center", justifyContent: "center",
    gap: 8, cursor: disabled ? "not-allowed" : "pointer",
    fontFamily: "'Syne', sans-serif", fontWeight: 600,
    fontSize: 13, letterSpacing: "0.3px",
    border: "none", borderRadius: 8, transition: "all 0.18s",
    opacity: disabled ? 0.5 : 1, width: full ? "100%" : undefined,
    padding: "10px 22px", ...style,
  };
  const variants = {
    primary: { background: T.amber,         color: "#000" },
    ghost:   { background: "transparent",   color: T.text,  border: `1px solid ${T.border}` },
    danger:  { background: T.red  + "20",   color: T.red,   border: `1px solid ${T.red}40`  },
    success: { background: T.green + "20",  color: T.green, border: `1px solid ${T.green}40` },
  };
  return (
    <button onClick={disabled ? undefined : onClick} style={{ ...base, ...variants[variant] }} {...props}>
      {children}
    </button>
  );
};

export const Input = ({
  label,
  type = "text",
  value,
  onChange,
  placeholder,
  icon,
  style,
  inputStyle,
  ...props
}) => (
  <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
    {label && (
      <label style={{ fontSize: 12, fontWeight: 500, color: T.muted, textTransform: "uppercase", letterSpacing: "0.8px" }}>
        {label}
      </label>
    )}
    <div style={{ position: "relative", ...style }}>
      {icon && (
        <span style={{ position: "absolute", left: 13, top: "50%", transform: "translateY(-50%)", color: T.muted, fontSize: 15 }}>
          {icon}
        </span>
      )}
      <input
        type={type}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        {...props}
        style={{
          width: "100%", background: T.navy3, border: `1px solid ${T.border}`,
          borderRadius: 8, color: T.white, padding: icon ? "11px 14px 11px 38px" : "11px 14px",
          fontSize: 14, fontFamily: "'DM Sans', sans-serif", outline: "none",
          transition: "border-color 0.2s",
          ...inputStyle,
        }}
        onFocus={e  => e.target.style.borderColor = T.amber}
        onBlur={e   => e.target.style.borderColor = T.border}
      />
    </div>
  </div>
);

export const Spinner = ({ size = 14, color = "#000" }) => (
  <span style={{
    width: size, height: size,
    border: `2px solid ${color}40`, borderTop: `2px solid ${color}`,
    borderRadius: "50%", animation: "spin 0.7s linear infinite",
    display: "inline-block", flexShrink: 0,
  }}/>
);

export const StatusPill = ({ status }) => {
  const map = {
    done:    [T.green, "● Done"],
    failed:  [T.red,   "✕ Failed"],
    running: [T.amber, "◌ Running"],
    pending: [T.muted, "◌ Pending"],
  };
  const [col, lbl] = map[status] || [T.muted, status];
  return <Badge color={col}>{lbl}</Badge>;
};
