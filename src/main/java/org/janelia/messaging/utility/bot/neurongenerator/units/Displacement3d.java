package org.janelia.messaging.utility.bot.neurongenerator.units;

public interface Displacement3d {
	PhysicalQuantity<BaseDimension.Length> getX();
	PhysicalQuantity<BaseDimension.Length> getY();
	PhysicalQuantity<BaseDimension.Length> getZ();
}
