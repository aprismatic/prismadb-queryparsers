using PrismaDB.QueryAST.DML;

namespace PrismaDB.QueryParser.Postgres
{
    public static class CnfConverter
    {
        public static ConjunctiveNormalForm BuildCnf(Expression expr)
        {
            var cnf = new ConjunctiveNormalForm();

            if (expr != null)
            {
                // If expression node is an AndClause, call BuildCNF recursively to add ANDs
                if (expr is AndClause and)
                {
                    cnf.AND.AddRange(BuildCnf(and.left).AND);
                    cnf.AND.AddRange(BuildCnf(and.right).AND);
                }
                else
                {
                    // If expression is an OrClause, build Disjunction and add to AND
                    cnf.AND.Add(BuildDisjunction(expr));
                }
            }

            return cnf;
        }

        public static Disjunction BuildDisjunction(Expression expr)
        {
            var disjunction = new Disjunction();
            if (expr != null)
            {
                if (expr is OrClause or)
                {
                    // If expression has more OR children, call BuildDisjunction recursively to add ORs
                    disjunction.OR.AddRange(BuildDisjunction(or.left).OR);
                    disjunction.OR.AddRange(BuildDisjunction(or.right).OR);
                }

                if (expr is BooleanExpression boolExpr)
                    disjunction.OR.Add(boolExpr);
            }

            return disjunction;
        }

        // Checks if expression tree is in full CNF form recursively.
        public static bool CheckCnf(Expression expr)
        {
            // If expression is empty, it is in CNF
            if (expr == null)
                return true;

            if (expr is OrClause or)
            {
                // If a child of an OrClause is an AndClause, it is not in CNF
                if (or.left is AndClause || or.right is AndClause)
                    return false;

                // Continue checking children (of children)
                return CheckCnf(or.left) && CheckCnf(or.right);
            }

            if (expr is AndClause and)
            {
                // AndClause can have either OrClause or AndClause as children
                return CheckCnf(and.left) && CheckCnf(and.right);
            }

            return true;
        }

        // Converts Expression tree to CNF form recursively.
        // May require several runs to reach full CNF form.
        public static Expression ConvertToCnf(Expression expr)
        {
            // If a child of an OrClause is an AndClause
            // Convert to CNF using distributive law
            // And continue to call ConvertToCnf recursively for the children
            if (expr is OrClause or)
            {
                if (or.left is AndClause leftAnd)
                {
                    var q = leftAnd.left.Clone() as Expression;
                    var r = leftAnd.right.Clone() as Expression;
                    var p = or.right.Clone() as Expression;

                    var newAnd = new AndClause(new OrClause(p, q), new OrClause(p, r));
                    newAnd.left = ConvertToCnf(newAnd.left);
                    newAnd.right = ConvertToCnf(newAnd.right);
                    return newAnd;
                }

                if (or.right is AndClause rightAnd)
                {
                    var q = rightAnd.left.Clone() as Expression;
                    var r = rightAnd.right.Clone() as Expression;
                    var p = or.left.Clone() as Expression;

                    var newAnd = new AndClause(new OrClause(p, q), new OrClause(p, r));
                    newAnd.left = ConvertToCnf(newAnd.left);
                    newAnd.right = ConvertToCnf(newAnd.right);
                    return newAnd;
                }

                or.left = ConvertToCnf(or.left);
                or.right = ConvertToCnf(or.right);
                return or;
            }

            if (expr is AndClause and)
            {
                and.left = ConvertToCnf(and.left);
                and.right = ConvertToCnf(and.right);
                return and;
            }

            return expr;
        }
    }
}