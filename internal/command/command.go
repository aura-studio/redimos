// Package command holds the command table, router, argument validation, and the
// per-family command handlers (strings, hashes, lists, sets, zsets, keys, stubs).
//
// The command table (CmdSpec/Table) and the router (Table.Dispatch) live in
// table.go and router.go; a Table implements server.Dispatcher and is wired
// into the server shell without the server package importing this one. The
// per-family handlers land in later tasks.
package command
