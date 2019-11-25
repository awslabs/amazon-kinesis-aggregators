package com.amazonaws.services.kinesis.aggregators.datastore.expressions;

import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;

public class UpdateExpression {

    // DOESN'T SUPPORT DELETES
    private Clause<SetAction> sets;
    private Clause<AddAction> adds;

    public UpdateExpression(Clause<SetAction> sets, Clause<AddAction> adds) {
        this.sets = sets;
        this.adds = adds;
    }

    @Override
    public String toString() {
        return this.sets.toString() + "\n" + this.adds.toString();
    }

    public static class Clause<T extends IAction> extends LinkedList<T> {

        @Override
        public String toString() {
            if (this.size() == 0)
                return "";

            String out = super.toString();
            return this.get(0).getAction() + " " + out.substring(1, out.length() - 1); // trim the "[]"
        }
    }
}
