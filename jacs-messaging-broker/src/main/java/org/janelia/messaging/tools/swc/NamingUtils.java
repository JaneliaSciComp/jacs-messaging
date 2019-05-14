package org.janelia.messaging.tools.swc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some useful functions for dealing with strings.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class NamingUtils {

    private static final Logger log = LoggerFactory.getLogger(NamingUtils.class);

    /**
     * Given some string starting with an integer, find next pos after.
     */
    static int findFirstNonDigitPosition(String inline) {
        int afterDigits = 0;
        while (Character.isDigit(inline.charAt(afterDigits))) {
            afterDigits++;
        }
        return afterDigits;
    }

    /**
     * Given some string ending with an integer, find where the int begins.
     */
    static int lastDigitPosition(String inline) {
        int beforeDigits = inline.length() - 1;
        while (Character.isDigit(inline.charAt(beforeDigits))) {
            beforeDigits--;
        }
        beforeDigits ++; // Move back to a convenient position.
        return beforeDigits;
    }

    /**
     * Given some filename, return an 'iterated' version, containing a counter
     * offset.  In this fashion, 'sub names' iterated over a count can be
     * generated from a 'parent name'.
     *
     * Example: mytext.txt -> mytext_1.txt   OR   mytext_2.txt
     *
     * @param baseFileName make a variant of this
     * @param offset use this offset in the new variant
     * @return the iterated filename.
     */
    static String getIteratedName(final String baseFileName, int offset) {
        String newName = baseFileName;
        int periodPos = newName.indexOf('.');
        if (periodPos > -1) {
            newName = newName.substring(0, periodPos)
                    + '_' + offset + newName.substring(periodPos);
        }
        return newName;
    }

}
