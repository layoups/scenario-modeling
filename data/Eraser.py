def erase(pdcts, session, table):
    if len(pdcts) == 0:
        graph = session.query(table)
    else:
        graph = session.query(table).filter(table.pdct_fam.in_(pdcts))
    for e in graph.all():
        e.ori_role = None
        e.desti_role = None
        e.color = None
        e.path = None
        e.pflow = None
        e.path_rank = None
        e.parent_pflow = None
        e.d = None
        e.f = None
        e.alpha = None
        e.in_pflow = None
    session.commit()

if __name__ == '__main__':

    None