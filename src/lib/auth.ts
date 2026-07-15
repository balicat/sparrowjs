// Auth: Basic bootstrap → Bearer adoption, the pattern validated against
// GizmoSQL, InfluxDB 3, Dremio and Sparrow. Sessions bind to the Bearer the
// server mints, so we adopt it from any response's headers — the same silent
// trick the ADBC drivers do.

function b64(s: string): string {
  if (typeof btoa === "function") return btoa(s);
  // Node < 16 fallback; every supported runtime has btoa, this is belt+braces
  return Buffer.from(s, "binary").toString("base64");
}

export interface AuthInit {
  user?: string;
  pass?: string;
  bearer?: string;
  headers?: Record<string, string>;
}

export interface CallOpts {
  headers: Record<string, string>;
  onHeader: (h: Headers) => void;
  signal?: AbortSignal;
}

export class AuthState {
  #auth: string | undefined;
  #extra: Record<string, string>;

  constructor(init: AuthInit) {
    this.#extra = init.headers ?? {};
    if (init.bearer) this.#auth = `Bearer ${init.bearer}`;
    else if (init.user !== undefined) this.#auth = "Basic " + b64(`${init.user}:${init.pass ?? ""}`);
  }

  /** connect-es call options: current auth header + Bearer adoption hook. */
  callOptions(signal?: AbortSignal): CallOpts {
    const headers: Record<string, string> = { ...this.#extra };
    if (this.#auth) headers.authorization = this.#auth;
    return {
      headers,
      onHeader: (h: Headers) => {
        const a = h.get("authorization");
        if (!a) return;
        // Headers.get comma-joins multiple values — take the Bearer token only
        const m = a.match(/Bearer\s+([^\s,]+)/i);
        this.#auth = m ? `Bearer ${m[1]}` : a;
      },
      signal,
    };
  }

  get isBearer(): boolean {
    return this.#auth?.startsWith("Bearer") ?? false;
  }
}
