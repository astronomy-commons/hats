from mocpy import MOC


def copy_moc(moc: MOC) -> MOC:
    """Returns a copy of a given MOC object"""
    return MOC.from_depth29_ranges(max_depth=moc.max_order, ranges=moc.to_depth29_ranges)
