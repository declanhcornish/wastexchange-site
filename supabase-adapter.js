/* WasteXchange: connects the site to Supabase (login, database, live updates, AI review).
   Exposes the small store interface the app code was written against. */
(function () {
  "use strict";
  const cfg = window.WX_CONFIG;
  const sb = window.supabase.createClient(cfg.supabaseUrl, cfg.supabaseAnonKey, {
    auth: { persistSession: true, autoRefreshToken: true }
  });
  let session = null;
  let adminFlag = null;
  let initialized = false;
  const ready = sb.auth.getSession().then(({ data }) => { session = data.session; initialized = true; });
  sb.auth.onAuthStateChange((event, s) => {
    // Events fired while the stored session is being restored are not a change of user.
    if (!initialized) { session = s; return; }
    const before = session?.user?.id || null;
    session = s;
    const after = s?.user?.id || null;
    if (before !== after) setTimeout(() => location.reload(), 0);
  });

  const uid = () => session?.user?.id || null;
  const authedOnly = new Set(["accounts", "threads", "messages"]);

  function route(path) {
    const p = path.split("/");
    if (p.length === 1) return { table: p[0] };
    if (p.length === 3 && p[0] === "threads" && p[2] === "messages") return { table: "messages", threadId: p[1] };
    throw new TypeError("Unsupported collection path: " + path);
  }
  const err = (e) => {
    const code = e?.code === "42501" || /row-level security|violates/i.test(e?.message || "") ? "invalid_argument" : "unavailable";
    return { code, message: e?.message || String(e) };
  };
  const snapDoc = (row) => ({ id: row.id, exists: true, data: () => row.data, metadata: { fromCache: false, hasPendingWrites: false } });
  const newId = () => (crypto.randomUUID ? crypto.randomUUID() : ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c => (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)));

  function docRef(table, id, threadId) {
    return {
      id, path: table + "/" + id,
      async get() {
        await ready;
        if (authedOnly.has(table) && !uid()) return { id, exists: false, data: () => undefined };
        const { data, error } = await sb.from(table).select("id,data").eq("id", id).maybeSingle();
        if (error) throw err(error);
        return data ? snapDoc(data) : { id, exists: false, data: () => undefined };
      },
      async set(body) {
        await ready;
        const row = { id, data: body };
        if (table === "messages") row.thread_id = threadId;
        if (table === "accounts") row.updated_at = new Date().toISOString();
        const { error } = await sb.from(table).upsert(row);
        if (error) throw err(error);
      },
      async update(patch) {
        await ready;
        const { data: cur, error: e1 } = await sb.from(table).select("data").eq("id", id).maybeSingle();
        if (e1) throw err(e1);
        if (!cur) throw { code: "invalid_argument", message: "Document does not exist" };
        const row = { data: { ...cur.data, ...patch } };
        if (table === "accounts") row.updated_at = new Date().toISOString();
        const { error } = await sb.from(table).update(row).eq("id", id);
        if (error) throw err(error);
      },
      async delete() { throw { code: "invalid_argument", message: "Delete is not supported" }; },
      collection(sub) { return collection(table + "/" + id + "/" + sub); }
    };
  }

  function collection(path, opts) {
    const r = route(path);
    const o = Object.assign({ order: null, dir: "asc", limit: 1000 }, opts || {});
    const q = {
      path,
      orderBy(field, dir) { return collection(path, { ...o, order: field, dir: dir || "asc" }); },
      limit(n) { return collection(path, { ...o, limit: n }); },
      where() { throw new TypeError("where() is not supported"); },
      doc(id) { return docRef(r.table, id || newId(), r.threadId); },
      async add(body) { const d = this.doc(); await d.set(body); return d; },
      async _fetch() {
        await ready;
        if (authedOnly.has(r.table) && !uid()) return [];
        let qb = sb.from(r.table).select("id,data");
        if (r.threadId) qb = qb.eq("thread_id", r.threadId);
        if (o.order) qb = qb.order("data->" + o.order, { ascending: o.dir !== "desc" });
        qb = qb.limit(o.limit);
        const { data, error } = await qb;
        if (error) throw err(error);
        return data || [];
      },
      async get() { const rows = await this._fetch(); return mkSnap(rows); },
      onSnapshot(next, onError) {
        let dead = false, timer = null;
        const run = async () => {
          try { const rows = await q._fetch(); if (!dead) next(mkSnap(rows)); }
          catch (e) { if (!dead && onError) onError(e); }
        };
        run();
        let ch = null;
        ready.then(() => {
          if (dead || (authedOnly.has(r.table) && !uid())) return;
          const filter = r.threadId ? { filter: "thread_id=eq." + r.threadId } : {};
          ch = sb.channel("wx-" + r.table + "-" + newId())
            .on("postgres_changes", { event: "*", schema: "public", table: r.table, ...filter }, () => {
              clearTimeout(timer); timer = setTimeout(run, 150);
            })
            .subscribe();
        });
        return () => { dead = true; clearTimeout(timer); if (ch) sb.removeChannel(ch); };
      }
    };
    return q;
  }
  function mkSnap(rows) {
    const docs = rows.map(snapDoc);
    return { docs, size: docs.length, empty: !docs.length, metadata: { fromCache: false, hasPendingWrites: false }, docChanges: () => [] };
  }

  const db = { doc: (p) => { const s = p.split("/"); if (s.length === 2) return docRef(s[0], s[1]); if (s.length === 4 && s[2] === "messages") return docRef("messages", s[3], s[1]); throw new TypeError("Unsupported document path: " + p); }, collection };

  const user = {
    async id() { await ready; return uid(); },
    async isOwner() {
      await ready; if (!uid()) return false;
      if (adminFlag === null) { const { data } = await sb.rpc("is_admin"); adminFlag = !!data; }
      return adminFlag;
    },
    async canEdit() { return this.isOwner(); },
    async can(name) { await ready; return name === "data.write" ? !!uid() : false; },
    async me() { await ready; return { id: uid(), name: "", email: session?.user?.email || null, isOwner: await this.isOwner(), canEdit: false }; },
    async profiles(ids) { return Object.fromEntries([].concat(ids).map(i => [i, { id: i, name: "", isMe: i === uid() }])); }
  };

  // AI compatibility review runs in the "ai-review" Edge Function, which holds the API key.
  async function sample() { throw { code: "not_granted", message: "Use sample.json" }; }
  sample.json = async (prompt) => {
    await ready;
    if (!uid()) throw { code: "not_granted", message: "Log in to run the review" };
    const { data, error } = await sb.functions.invoke("ai-review", { body: { prompt } });
    if (error) {
      let code = "unavailable";
      try { const b = await error.context?.json?.(); if (b?.error) code = b.error; } catch {}
      throw { code, message: error.message };
    }
    if (data?.error) throw { code: data.error, message: data.error };
    return data;
  };

  window.WX = {
    sb,
    ready,
    email: () => session?.user?.email || null,
    signIn: (email, password) => sb.auth.signInWithPassword({ email, password }),
    signUp: (email, password) => sb.auth.signUp({ email, password, options: { emailRedirectTo: location.origin + location.pathname } }),
    signOut: () => sb.auth.signOut()
  };
  window.claude = { use: async (name) => (name === "db" ? db : name === "user" ? user : name === "sample" ? sample : null) };
})();
