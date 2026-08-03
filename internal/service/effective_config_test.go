package service

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The override feature shipped broken because CreateChildJob never set
// DeploymentTargetID: the child job is what calls the provider, so with no
// target id resolveEffectiveConfig always fell through to the configuration's
// own config and deployed to the wrong Cloudflare zone.
//
// Every CreateChildJob call site must therefore pass a deployment target. This
// walks the AST rather than grepping so a renamed variable or reformatted call
// cannot make the check pass vacuously.
func TestEveryCreateChildJobCallPassesADeploymentTarget(t *testing.T) {
	// (ctx, tenantID, parentJobID, deploymentTargetID, targetConfigurationID, ...)
	const deploymentTargetArgIndex = 3
	const minArgs = 9

	roots := []string{".", "../event"}
	found := 0

	for _, root := range roots {
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, root, func(fi os.FileInfo) bool {
			return !strings.HasSuffix(fi.Name(), "_test.go")
		}, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", root, err)
		}

		for _, pkg := range pkgs {
			for path, file := range pkg.Files {
				ast.Inspect(file, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "CreateChildJob" {
						return true
					}
					found++

					if len(call.Args) < minArgs {
						t.Errorf("%s:%d: CreateChildJob called with %d args, want at least %d — the deployment target argument is missing",
							filepath.Base(path), fset.Position(call.Pos()).Line, len(call.Args), minArgs)
						return true
					}

					arg := call.Args[deploymentTargetArgIndex]
					if lit, isLit := arg.(*ast.BasicLit); isLit && lit.Kind == token.STRING {
						if lit.Value == `""` {
							t.Errorf("%s:%d: CreateChildJob passes an empty deployment target; config overrides would be silently ignored",
								filepath.Base(path), fset.Position(call.Pos()).Line)
						}
					}
					return true
				})
			}
		}
	}

	if found == 0 {
		t.Fatal("no CreateChildJob call sites found — this test is not actually guarding anything")
	}
	t.Logf("checked %d CreateChildJob call site(s)", found)
}
