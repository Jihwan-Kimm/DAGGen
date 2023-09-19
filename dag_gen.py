from random import randint
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import random

# get [mean, stdev] and return mean +- stdev random number
def rand_uniform(arr):
    if arr[1] < 0:
        raise ValueError('should pass positive stdev : %d, %d' % (arr[0], arr[1]))

    return randint(int(arr[0] - arr[1]), int(arr[0] + arr[1]))

class Task(object):
    idx = 0
    def __init__(self, **kwargs):
        self.tid = Task.idx
        Task.idx += 1
        self.name = kwargs.get('name', '')
        self.exec_t = kwargs.get('exec_t', 30.0)

        # Assigned after DAGGen
        self.parent = []
        self.child = []
        self.isLeaf = False
        # self.deadline = -1
        self.level = 0

        self.est = -1
        self.ltc = -1
        self.i = -1
        self.f = -1

        self.color=(random.random()/2.5+0.2, random.random()/2.5+0.2, random.random()/2.5+0.2)

    def __str__(self):
        # res = "%-9s exec_t : %.1f, parent : %20s child : %30s" \
        #     % ('[' + self.name + ']', self.exec_t, self.parent, self.child)

        # if self.isLeaf:
        #     res += " DL : %s" % (self.deadline)

        res = "%-9s %-5.1f %40s %40s" \
            % ('[' + self.name + ']', self.exec_t, self.parent, self.child)

        res += "%5s" % (self.est)
        res += "%5s" % (self.ltc)

        return res

    def new_task_set(self):
        Task.idx = 0

class DAGGen(object):
    def __init__(self, arti, **kwargs):

        if arti:
            self.task_set=[
                Task(name="node 0", exec_t=35),
                Task(name="node 1", exec_t=40),
                Task(name="node sl", exec_t=135),
                Task(name="node 3", exec_t=40),
                Task(name="node 4", exec_t=50),
                Task(name="node 5", exec_t=20),
                Task(name="node 6", exec_t=30),
                Task(name="node 7", exec_t=25),
                Task(name="node 8", exec_t=20),
                Task(name="node 9", exec_t=30),
                Task(name="node 10", exec_t=25),
            ]
            self.task_set[0].child.append(1)
            self.task_set[0].child.append(5)
            self.task_set[0].child.append(8)

            self.task_set[1].child.append(2)
            self.task_set[1].child.append(9)
            
            self.task_set[2].child.append(3)
            self.task_set[2].child.append(10)

            self.task_set[3].child.append(4)
            

            self.task_set[5].child.append(6)
            self.task_set[5].child.append(3)

            self.task_set[6].child.append(4)
            self.task_set[6].child.append(10)
            self.task_set[7].child.append(2)
            self.task_set[7].child.append(8)
            self.task_set[8].child.append(3)
            self.task_set[8].child.append(9)
            self.task_set[9].child.append(3)

            self.task_set[1].parent.append(0)

            self.task_set[2].parent.append(1)
            self.task_set[2].parent.append(7)


            self.task_set[3].parent.append(2)
            self.task_set[3].parent.append(5)
            self.task_set[3].parent.append(8)
            self.task_set[3].parent.append(9)

            self.task_set[4].parent.append(3)

            self.task_set[5].parent.append(0)

            self.task_set[6].parent.append(5)

            self.task_set[8].parent.append(7)
            self.task_set[8].parent.append(0)
            self.task_set[9].parent.append(8)
            self.task_set[9].parent.append(1)

            self.task_set[10].parent.append(2)
            self.task_set[10].parent.append(6)

            self.node_num=11
            node_num=11
            self.node_est = []
        else:
                



            self.node_num = kwargs.get('node_num', [40, 10])
            self.depth = kwargs.get('depth', [3.5, 0.5])
            self.exec_t = kwargs.get('exec_t', [50.0, 30.0])
            self.start_node = kwargs.get('start_node', [2, 1])
            self.edge_num_constraint = kwargs.get('edge_constraint', False)

            # Use when edge_num_constraint is True
            # self.inbound_num = kwargs.get('inbound_num', [2, 0])
            self.outbound_num = kwargs.get('outbound_num', [3, 0])

            # Use when edge_num_constraint is False
            self.extra_arc_ratio = kwargs.get('extra_arc_ratio', 0.1)

            self.task_set = []
            self.node_est = []

            ### 1. Initialize Task
            node_num = rand_uniform(self.node_num)
            for i in range(node_num):
                task_param = {
                    "name" : "node" + str(i),
                    "exec_t" : rand_uniform(self.exec_t)
                }

                self.task_set.append(Task(**task_param))

            depth = rand_uniform(self.depth)

            extra_arc_num = int(node_num * self.extra_arc_ratio)

            ### 2. Classify Task by randomly-select level
            level_arr = []
            for i in range(depth):
                level_arr.append([])

            # put start nodes in level 0
            start_node_num = rand_uniform(self.start_node)
            for i in range(start_node_num):
                level_arr[0].append(i)
                self.task_set[i].level = 0
            
            # Each level must have at least one node
            for i in range(1, depth):
                level_arr[i].append(start_node_num + i - 1)
                self.task_set[start_node_num+i-1].level = i

            # put other nodes in other level randomly
            for i in range(start_node_num + depth - 1, node_num):
                level = randint(1, depth-1)
                self.task_set[i].level = level
                level_arr[level].append(i)

            ### 3-(A). When edge_num_constraint is True
            if self.edge_num_constraint:
                ### make arc for first level
                for level in range(0, depth-1):
                    for task_idx in level_arr[level]:
                        ob_num = rand_uniform(self.outbound_num)

                        child_idx_list = []

                        # if desired outbound edge number is larger than the number of next level nodes, select every node
                        if ob_num >= len(level_arr[level + 1]):
                            child_idx_list = level_arr[level + 1]
                        else:
                            while len(child_idx_list) < ob_num:
                                child_idx = level_arr[level+1][randint(0, len(level_arr[level + 1])-1)]
                                if child_idx not in child_idx_list:
                                    child_idx_list.append(child_idx)
                        
                        for child_idx in child_idx_list:
                            self.task_set[task_idx].child.append(child_idx)
                            self.task_set[child_idx].parent.append(task_idx)

            ### 3-(B). When edge_num_constraint is False
            else:
                ### make arc from last level
                for level in range(depth-1, 0, -1):
                    for task_idx in level_arr[level]:
                        parent_idx = level_arr[level-1][randint(0, len(level_arr[level - 1])-1)]

                        self.task_set[parent_idx].child.append(task_idx)
                        self.task_set[task_idx].parent.append(parent_idx)

                ### make extra arc
                for i in range(extra_arc_num):
                    arc_added_flag = False
                    while not arc_added_flag:
                        task1_idx = randint(0, node_num-1)
                        task2_idx = randint(0, node_num-1)

                        if self.task_set[task1_idx].level < self.task_set[task2_idx].level:
                            self.task_set[task1_idx].child.append(task2_idx)
                            self.task_set[task2_idx].parent.append(task1_idx)
                            arc_added_flag = True
                        elif self.task_set[task1_idx].level > self.task_set[task2_idx].level:
                            self.task_set[task2_idx].child.append(task1_idx)
                            self.task_set[task1_idx].parent.append(task2_idx)
                            arc_added_flag = True
            
            ### 5. set deadline ( exec_t avg * (level + 1)) * 2
            # for task in self.task_set:
            #     if len(task.child) == 0:
            #         task.isLeaf = True
            #         task.deadline = self.exec_t[0] * (task.level+1) * 2

            # calculate est & critical path
        for i in range(node_num):
            self.calc_est(i)
        max_est=0
        max_est_i=0
        for i in range(node_num):
            self.node_est.append(self.task_set[i].est)
            if self.node_est[i]+self.task_set[i].exec_t>max_est:
                max_est=self.node_est[i]+self.task_set[i].exec_t
                max_est_i=i

        self.critical_path=[]
        self.checkpoint=[max_est]
        max_est-=self.task_set[max_est_i].exec_t
        while (max_est!=0) :
            self.critical_path.append(max_est_i)
            for j in self.task_set[max_est_i].parent:
                if self.node_est[j]+self.task_set[j].exec_t==max_est:
                    max_est_i=j
                    self.checkpoint.append(max_est)
                    max_est-=self.task_set[j].exec_t
                    break
        self.checkpoint.append(0)
        self.critical_path.append(max_est_i)
        self.checkpoint.reverse()
        self.critical_path.reverse()

        for i in range(node_num):
            self.calc_ltc(i)


    def __str__(self):
        # print("%-9s %-5s %39s %40s %8s %5s %5s" % ('name', 'exec_t', 'parent node', 'child node', 'deadline', 'est', 'ltc'))
        print("%-9s %-5s %39s %40s %5s %5s" % ('name', 'exec_t', 'parent node', 'child node', 'est', 'ltc'))
        for task in self.task_set:
            print(task)
        print("Critical path : ", self.critical_path)
        print("Checkpoint : ", self.checkpoint)

        return ''

    def calc_est(self, task_idx):
        if self.task_set[task_idx].est == -1 :
            if len(self.task_set[task_idx].parent)==0 :
                self.task_set[task_idx].est=0
                self.task_set[task_idx].i=0
            else :
                est=0
                for i in self.task_set[task_idx].parent:
                    if self.task_set[i].est == -1:
                        self.calc_est(self.task_set[i].tid)
                    if self.task_set[i].est+self.task_set[i].exec_t>est:
                        est=self.task_set[i].est+self.task_set[i].exec_t
                self.task_set[task_idx].est=est
                self.task_set[task_idx].i=est

    def calc_ltc(self, task_idx):
        if self.task_set[task_idx].ltc == -1 :
            if len(self.task_set[task_idx].child)==0 :
                self.task_set[task_idx].ltc=self.checkpoint[-1]
                self.task_set[task_idx].f=self.checkpoint[-1]
            else :
                ltc=self.checkpoint[-1]
                for i in self.task_set[task_idx].child:
                    if self.task_set[i].ltc == -1:
                        self.calc_ltc(self.task_set[i].tid)
                    if self.task_set[i].ltc-self.task_set[i].exec_t<ltc:
                        ltc=self.task_set[i].ltc-self.task_set[i].exec_t
                self.task_set[task_idx].ltc=ltc
                self.task_set[task_idx].f=ltc

    def draw_est(self, title):
        fig, ax = plt.subplots()
        for i in range(len(self.checkpoint)-1):
            ax.add_patch(Rectangle((self.checkpoint[i], 0), self.checkpoint[i+1]-self.checkpoint[i], 15, fill=False, color='black'))
            ax.text((self.checkpoint[i]+self.checkpoint[i+1]) / 2, 7, self.task_set[self.critical_path[i]].name, ha='center', va='center', color='black')
        for i in range(len(self.task_set)):
            if i in self.critical_path: continue
            ax.add_patch(Rectangle((self.task_set[i].est, 20+(i-len(self.critical_path))*15), self.task_set[i].ltc-self.task_set[i].est, 15, fill=False, color='black'))
            ax.add_patch(Rectangle((self.task_set[i].est, 20+(i-len(self.critical_path))*15), self.task_set[i].exec_t, 15, color=self.task_set[i].color))
            ax.text(self.task_set[i].est+20, 27+(i-len(self.critical_path))*15, self.task_set[i].name, ha='center', va='center', color='black')
        plt.axis('equal') 
        plt.savefig(title, dpi=300)

    def draw_pulled(self, title):
        fig, ax = plt.subplots()
        for i in range(len(self.checkpoint)-1):
            ax.add_patch(Rectangle((self.checkpoint[i], 0), self.checkpoint[i+1]-self.checkpoint[i], 15, fill=False, color='black'))
            ax.text((self.checkpoint[i]+self.checkpoint[i+1]) / 2, 7, self.task_set[self.critical_path[i]].name, ha='center', va='center', color='black')
        for i in range(len(self.task_set)):
            if i in self.critical_path: continue
            ax.add_patch(Rectangle((self.task_set[i].est, 20+(i-len(self.critical_path))*15), self.task_set[i].ltc-self.task_set[i].est, 15, fill=False, color='black'))
            ax.add_patch(Rectangle((self.task_set[i].i, 20+(i-len(self.critical_path))*15), self.task_set[i].f-self.task_set[i].i, 15*self.task_set[i].exec_t/(self.task_set[i].f-self.task_set[i].i), color=self.task_set[i].color))
            ax.text(self.task_set[i].est+20, 27+(i-len(self.critical_path))*15, self.task_set[i].name, ha='center', va='center', color='black')
        plt.axis('equal') 
        plt.savefig(title, dpi=300)
    
    def check_depen(self):
        for i in range(len(self.task_set)):
            for j in self.task_set[i].parent:
                if self.task_set[i].i<self.task_set[j].f:
                    border=(self.task_set[i].exec_t*self.task_set[j].i+self.task_set[j].exec_t*self.task_set[i].f)/(self.task_set[i].exec_t+self.task_set[j].exec_t)
                    if border<self.task_set[i].est: border=self.task_set[i].est
                    if border>self.task_set[j].ltc: border=self.task_set[j].ltc
                    self.task_set[i].i=border+1
                    self.task_set[j].f=border
                    print("child %d parent %d border %f" % (i, j, border))

            for j in self.task_set[i].child:
                if self.task_set[i].f>self.task_set[j].i:
                    border=(self.task_set[j].exec_t*self.task_set[i].i+self.task_set[i].exec_t*self.task_set[j].f)/(self.task_set[i].exec_t+self.task_set[j].exec_t)
                    if border>self.task_set[i].ltc: border=self.task_set[i].ltc
                    if border<self.task_set[j].est: border=self.task_set[j].est
                    self.task_set[i].f=border
                    self.task_set[j].i=border+1
                    print("parent %d child %d border %f"% (i, j, border))
    def print_U(self):
        for i in range(len(self.task_set)):
            print(i,  self.task_set[i].exec_t,self.task_set[i].f-self.task_set[i].i, self.task_set[i].exec_t/(self.task_set[i].f-self.task_set[i].i))


if __name__ == "__main__":
    dag_param_1 = {
        "node_num" : [10, 00],
        "depth" : [4.5, 0.5],
        "exec_t" : [50.0, 30.0],
        "start_node" : [2, 1],
        "extra_arc_ratio" : 0.4
    }

    dag_param_2 = {
        "node_num" : [20, 0],
        "depth" : [4.5, 0.5],
        "exec_t" : [50.0, 30.0],
        "start_node" : [2, 0],
        "edge_constraint" : True,
        "outbound_num" : [2, 0]
    }

    # dag = DAGGen(**dag_param_1)
    dag = DAGGen(True, **dag_param_1,)

    print(dag)
    dag.draw_est('fig/bound.png')
    dag.draw_pulled('fig/stretched.png')
    dag.check_depen()
    dag.print_U()
    dag.draw_pulled('fig/depen.png')


