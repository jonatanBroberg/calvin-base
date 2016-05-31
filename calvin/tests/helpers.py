from calvin.utilities import utils


def actual_tokens(rt, actor_id):
    return utils.report(rt, actor_id)


def expected_counter(n):
    return [i for i in range(1, n + 1)]


def cumsum(l):
    s = 0
    for n in l:
        s = s + n
        yield s


def expected_sum(n):
    return list(cumsum(range(1, n + 1)))


def expected_tokens(rt, actor_id, src_actor_type):
    tokens = utils.report(rt, actor_id)

    if src_actor_type == 'std.CountTimer':
        return expected_counter(tokens)

    if src_actor_type == 'std.ReplicatedCountTimer':
        return expected_counter(tokens)

    if src_actor_type == 'std.Sum':
        return expected_sum(tokens)

    return None
