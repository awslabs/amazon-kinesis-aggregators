package com.amazonaws.services.kinesis.aggregators.datastore.expressions;

public class SetAction implements IAction {

    public static class Operand {
        Value plus(Operand other) {
            return new Sum(this, other);
        }

        Value minus(Operand other) {
            return new Sub(this, other);
        }
    }

    public static class Append extends Operand {
        private String left, right;
        public Append(String left, String right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return "list_append(" + left + ", " + right + ")";
        }
    }

    public static class IfNotExists extends Operand {
        private String left, right;
        public IfNotExists(String left, String right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return "if_not_exists(" + left + ", " + right + ")";
        }
    }

    public static class Path extends Operand {
        private String path;
        public Path(String path) {
            this.path = path;
        }

        @Override
        public String toString() {
            return path;
        }
    }

    static interface Value {}

    public static class Raw implements Value {
        Operand operand;
        public Raw(Operand operand) {
            this.operand = operand;
        }

        @Override
        public String toString() {
            return this.operand.toString();
        }
    }

    public static class Sum implements Value {
        Operand left, right;
        public Sum(Operand left, Operand right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return " " + this.left.toString() + " + " + this.right.toString() + " ";
        }
    }

    public static class Sub implements Value {
        Operand left, right;
        public Sub(Operand left, Operand right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return " " + this.left.toString() + " - " + this.right.toString() + " ";
        }
    }

    String path;
    Value rhs;

    public SetAction(String path, Value rhs) {
            this.path = path;
            this.rhs = rhs;
    }

    public SetAction(String path, String rhs) {
        this(path, new Raw(new Path(rhs)));
    }

    @Override
    public String getAction() {
        return "SET";
    }

    @Override
    public String toString() {
        return path + " = " + rhs.toString();
    }
}
