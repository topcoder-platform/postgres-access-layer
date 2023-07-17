package com.topcoder.pal.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class ParameterizedExpression {
    private String expression;
    private Object[] parameter = new Object[0];
}
