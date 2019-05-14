package org.janelia.messaging.tools.units;

/**
 * Physical unit like "meter" or "kilogram" or "meters per second"
 *
 * @param <D>
 * @author brunsc
 */
public interface PhysicalUnit<D extends PhysicalDimension> {
    String toString(); // name of unit

    String getSymbol(); // symbol abbreviation
}
