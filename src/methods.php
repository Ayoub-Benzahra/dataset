<?php

use Illuminate\Support\Str;

if (!function_exists('str_ordinal')) {
    /**
     * Author: https://tenerant.com/blog/laravel-ordinal-helper-function/
     * Append an ordinal indicator to a numeric value.
     *
     * @param string|int $value
     *
     * @return string
     */
    function str_ordinal ($value) {
        $number = abs($value);

        $indicators = [ 'th', 'st', 'nd', 'rd', 'th', 'th', 'th', 'th', 'th', 'th' ];

        $suffix = $indicators[$number % 10];
        if ($number % 100 >= 11 && $number % 100 <= 13) {
            $suffix = 'th';
        }

        return number_format($number) . $suffix;
    }
}

if (!function_exists('morph_class_name')) {
    function morph_class_name ($klass) {
        return Str::snake(Str::pluralStudly(class_basename($klass)));
    }
}

if (!function_exists('is_multidimensional_array')) {
    function is_multidimensional_array ($array) {
        return (array_values($array) !== $array);
    }
}
