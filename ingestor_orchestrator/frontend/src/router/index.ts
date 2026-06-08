import { createRouter, createWebHistory } from "vue-router";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: "/",
      name: "dashboard",
      component: () => import("@/views/DashboardView.vue"),
    },
    {
      path: "/jobs",
      name: "jobs",
      component: () => import("@/views/JobsView.vue"),
    },
    {
      path: "/jobs/:id",
      name: "job-detail",
      component: () => import("@/views/JobDetailView.vue"),
      props: true,
    },
  ],
});

export default router;
