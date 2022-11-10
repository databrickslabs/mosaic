
def scala_map_to_dict(scala_map):
    result = dict()
    for i in range(0, scala_map.size()):
        current = scala_map.apply(i)
        curr_key = current._1()
        curr_val = current._2()
        result[curr_key] = curr_val
    return result