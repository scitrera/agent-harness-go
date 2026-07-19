package tools

import "context"

// SkillRealizerFunc materializes the named skills' bundle files — utils.py,
// references/, assets/, and the shared bundle — into the distribution's skills
// root (e.g. the sandbox's shared /skills directory), so code running in the
// code-sidecar (e.g. import_path('/skills/<name>/utils.py'), and skill bodies
// citing that root) resolves them. load_skill
// calls it with the target skill plus its prerequisites. It is best-effort and
// idempotent: it returns the number of files realized; absence on ctx (no
// materialize dir / non-relay transport) is a silent no-op.
//
// Like ModelPreference and WorldStateSink, this is a request-scoped capability the
// runner installs on ctx rather than threading a materialization dependency through
// every tool signature. The realization SOURCE (MemoryLayer bundle files, a
// workspace skill folder, an image-baked root) is the distribution's concern; the
// tool just names the skills it loaded.
type SkillRealizerFunc func(ctx context.Context, names []string) (int, error)

type skillRealizerKey struct{}

// WithSkillRealizer returns a context carrying the per-turn skill realizer.
func WithSkillRealizer(ctx context.Context, fn SkillRealizerFunc) context.Context {
	return context.WithValue(ctx, skillRealizerKey{}, fn)
}

// SkillRealizerFrom returns the skill realizer carried on ctx, and whether one was
// present. Absence is a no-op (e.g. a transport with no skills materialization).
func SkillRealizerFrom(ctx context.Context) (SkillRealizerFunc, bool) {
	fn, ok := ctx.Value(skillRealizerKey{}).(SkillRealizerFunc)
	return fn, ok
}
