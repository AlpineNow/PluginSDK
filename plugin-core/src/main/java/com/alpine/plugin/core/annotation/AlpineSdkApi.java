/**
 * COPYRIGHT (C) 2015 Alpine Data Labs Inc. All Rights Reserved.
 */

package com.alpine.plugin.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for public-facing SDK APIs. All the publicly usable APIs (classes,
 * functions, member variables, etc.) should be annotated with this.
 *
 * NOTE: If there exists a Scaladoc comment that immediately precedes this
 * annotation, the first line of the comment must be ":: AlpineSdkApi ::" with
 * no trailing blank line. This is because of the known issue that Scaladoc
 * displays only either the annotation or the comment, whichever comes first.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(
    {
        ElementType.TYPE, ElementType.FIELD, ElementType.METHOD,
        ElementType.PARAMETER, ElementType.CONSTRUCTOR,
        ElementType.LOCAL_VARIABLE, ElementType.PACKAGE
    }
)
public @interface AlpineSdkApi {}
