using PrismaDB.QueryAST.DML;

namespace PrismaDB.QueryParser.MSSQL
{
    public partial class MsSqlParser
    {
        /// <summary>
        ///     Checks if expression tree is in full CNF form recursively.
        /// </summary>
        /// <param name="expr">Expression tree root</param>
        /// <returns>Whether expression tree is in CNF</returns>
        private static bool CheckCNF(Expression expr)
        {
            // If expression is empty, it is in CNF
            // Or terminates if is a leaf 
            if (expr == null)
                return true;

            if (expr.GetType() == typeof(OrClause))
            {
                var or = (OrClause) expr;
                // If a child of an OrClause is an AndClause, it is not in CNF
                if (or.left.GetType() == typeof(AndClause))
                    return false;
                if (or.right.GetType() == typeof(AndClause))
                    return false;

                // Continue checking children (of children)
                return CheckCNF(or.left) && CheckCNF(or.right);
            }

            if (expr.GetType() == typeof(AndClause))
            {
                // AndClause can have either OrClause or AndClause as children
                var and = (AndClause) expr;
                return CheckCNF(and.left) && CheckCNF(and.right);
            }

            return true;
        }


        /// <summary>
        ///     Build ConjunctiveNormalForm object from Expression tree.
        /// </summary>
        /// <param name="expr">Expression tree</param>
        /// <returns>ConjunctiveNormalForm object</returns>
        private static ConjunctiveNormalForm BuildCNF(Expression expr)
        {
            var cnf = new ConjunctiveNormalForm();

            if (expr != null)
            {
                // If expression node is an AndClause, call BuildCNF recursively to add ANDs
                if (expr.GetType() == typeof(AndClause))
                {
                    cnf.AND.AddRange(BuildCNF(((AndClause) expr).left).AND);
                    cnf.AND.AddRange(BuildCNF(((AndClause) expr).right).AND);
                }
                else
                {
                    // If expression is an OrClause, build Disjunction and add to AND
                    cnf.AND.Add(BuildDisjunction(expr));
                }
            }

            return cnf;
        }


        /// <summary>
        ///     Build Disjunction object from Expression tree.
        /// </summary>
        /// <param name="expr">OR Expression tree</param>
        /// <returns>Disjunction object</returns>
        private static Disjunction BuildDisjunction(Expression expr)
        {
            var disjunction = new Disjunction();
            if (expr != null)
            {
                if (expr.GetType() == typeof(OrClause))
                {
                    // If expression has more OR children, call BuildDisjunction recursively to add ORs
                    disjunction.OR.AddRange(BuildDisjunction(((OrClause) expr).left).OR);
                    disjunction.OR.AddRange(BuildDisjunction(((OrClause) expr).right).OR);
                }
                
                if (expr is BooleanExpression boolExpr)
                    disjunction.OR.Add(boolExpr);
            }

            return disjunction;
        }


        /// <summary>
        ///     Converts Expression tree to CNF form recursively.
        ///     May require several runs to reach full CNF form.
        /// </summary>
        /// <param name="expr">Expression tree node</param>
        /// <returns>Expression tree in CNF form</returns>
        private static Expression ConvertToCNF(Expression expr)
        {
            // If a child of an OrClause is an AndClause
            // Convert to CNF using distributive law
            // And continue to call ConvertToCNF recursively for the children
            if (expr.GetType() == typeof(OrClause))
            {
                var or = (OrClause) expr;
                if (or.left.GetType() == typeof(AndClause))
                {
                    var childAnd = (AndClause) or.left;

                    var q = childAnd.left.Clone() as Expression;
                    var r = childAnd.right.Clone() as Expression;
                    var p = or.right.Clone() as Expression;

                    var newAnd = new AndClause(new OrClause(p, q), new OrClause(p, r));
                    newAnd.left = ConvertToCNF(newAnd.left);
                    newAnd.right = ConvertToCNF(newAnd.right);
                    return newAnd;
                }

                if (or.right.GetType() == typeof(AndClause))
                {
                    var childAnd = (AndClause) or.right;

                    var q = childAnd.left.Clone() as Expression;
                    var r = childAnd.right.Clone() as Expression;
                    var p = or.left.Clone() as Expression;

                    var newAnd = new AndClause(new OrClause(p, q), new OrClause(p, r));
                    newAnd.left = ConvertToCNF(newAnd.left);
                    newAnd.right = ConvertToCNF(newAnd.right);
                    return newAnd;
                }

                or.left = ConvertToCNF(or.left);
                or.right = ConvertToCNF(or.right);
                return or;
            }

            if (expr.GetType() == typeof(AndClause))
            {
                var and = (AndClause) expr;
                and.left = ConvertToCNF(and.left);
                and.right = ConvertToCNF(and.right);
                return and;
            }

            return expr;
        }
    }
}