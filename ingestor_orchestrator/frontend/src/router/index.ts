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
    {
      path: "/resources",
      name: "resources",
      component: () => import("@/views/ResourcesView.vue"),
    },
    {
      path: "/resources/:resourceId",
      name: "resource-detail",
      component: () => import("@/views/ResourceDetailView.vue"),
      props: true,
    },
    {
      path: "/syncs",
      name: "syncs",
      component: () => import("@/views/SyncsView.vue"),
    },
  ],
});

export default router;
