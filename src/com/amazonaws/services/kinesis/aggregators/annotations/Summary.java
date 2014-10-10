package com.amazonaws.services.kinesis.aggregators.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;

import com.amazonaws.services.kinesis.aggregators.summary.SummaryCalculation;

/**
 * Annotation which indicates that a method should be used as a summary
 * aggregation. If no type is indicated then it will be used as a
 * {@link com.amazonaws.services.kinesis.aggregators.SummaryCalculation.SUM}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Summary {
    /**
     * The type of summary calculations to apply to the method.
     * 
     * @return
     */
    public SummaryCalculation[] type() default SummaryCalculation.SUM;
}
