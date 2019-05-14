package org.janelia.messaging.tools.units;

public interface PhysicalQuantity<D extends PhysicalDimension> {
    PhysicalUnit<D> getUnit();

    double getValue();
}
