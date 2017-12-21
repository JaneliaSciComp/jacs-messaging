package org.janelia.messaging.utility.bot.neurongenerator.units;

public interface PhysicalQuantity<D extends PhysicalDimension>
{
	PhysicalUnit<D> getUnit();
	double getValue();
}
